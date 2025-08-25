# // name field added
#improved UI
# // change the storage to Srt-model/

import os
import io
import uuid
import socket
import mimetypes
import time
import json
from typing import Dict, Any, Optional
from datetime import datetime, timezone

import streamlit as st
from dotenv import load_dotenv
import requests
import boto3
from botocore.client import Config
from botocore.exceptions import EndpointConnectionError, ClientError

# =========================================================
# Secrets loader (works with .env locally and st.secrets on Cloud)
# =========================================================
load_dotenv()  # safe no-op on Cloud

def _sget(key: str, default: Optional[str] = None, section: Optional[str] = None) -> Optional[str]:
    """
    Priority:
      1) Environment variables (local / Docker)
      2) st.secrets[section][key] or st.secrets[key] (Streamlit Cloud)
      3) default
    """
    v = os.getenv(key)
    if v not in (None, ""):
        return v
    try:
        if section:
            sect = st.secrets.get(section, {})
            if isinstance(sect, dict) and key in sect and sect[key]:
                return sect[key]
        # fall back to flat top-level secret
        v2 = st.secrets.get(key, default)
        return v2 if v2 not in ("", None) else default
    except Exception:
        return default

def _require(name: str, value: Optional[str]):
    if not value:
        st.error(f"Missing secret: **{name}**. Add it in `.streamlit/secrets.toml` or in **App → Settings → Secrets**.")
        st.stop()

# =========================================================
# Read secrets (supports both flat keys and [runpod_s3] group)
# =========================================================
RUNPOD_API_KEY   = _sget("RUNPOD_API_KEY")
RUNPOD_ENDPOINT  = _sget("RUNPOD_ENDPOINT_ID")

# S3: try grouped first, then flat
S3_REGION = _sget("RUNPOD_S3_REGION", section="runpod_s3") or _sget("RUNPOD_S3_REGION")
S3_BUCKET = _sget("RUNPOD_S3_BUCKET", section="runpod_s3") or _sget("RUNPOD_S3_BUCKET")
S3_KEY    = _sget("RUNPOD_S3_ACCESS_KEY", section="runpod_s3") or _sget("RUNPOD_S3_ACCESS_KEY")
S3_SECRET = _sget("RUNPOD_S3_SECRET_KEY", section="runpod_s3") or _sget("RUNPOD_S3_SECRET_KEY")
# Fail fast with a helpful UI message instead of crashing the app
for k, v in {
    "RUNPOD_API_KEY": RUNPOD_API_KEY,
    "RUNPOD_ENDPOINT_ID": RUNPOD_ENDPOINT,
    "RUNPOD_S3_REGION": S3_REGION,
    "RUNPOD_S3_BUCKET": S3_BUCKET,
    "RUNPOD_S3_ACCESS_KEY": S3_KEY,
    "RUNPOD_S3_SECRET_KEY": S3_SECRET,
}.items():
    _require(k, v)

RUN_URL         = f"https://api.runpod.ai/v2/{RUNPOD_ENDPOINT}/run"
STATUS_URL_BASE = f"https://api.runpod.ai/v2/{RUNPOD_ENDPOINT}/status/"
DEFAULT_HEADERS = lambda key: {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

# =========================================================
# Page + styles (set_page_config must be first Streamlit call)
# =========================================================
st.set_page_config(page_title="SRT Generator ", page_icon="🎧", layout="wide", initial_sidebar_state="expanded",)

st.markdown("""
<style>
# button[aria-label="Hide sidebar"],
# button[aria-label="Show sidebar"],
# [data-testid="collapsedControl"],
# [data-testid="stSidebarCollapseButton"] { display: none !important; }

# section[data-testid="stSidebar"] { transform: none !important; visibility: visible !important; opacity: 1 !important; }
# @media (max-width: 1200px) { section[data-testid="stSidebar"] { position: sticky !important; left: 0 !important; } }
# section[data-testid="stSidebar"] { min-width: 280px; max-width: 3200px; }

/* File grid buttons */
# file-list button[data-baseweb="button"]{
    height: 70px !important; border-radius: 18px !important; font-weight: 700 !important; font-size: 20px !important;
    letter-spacing: .2px; border: 1px solid rgba(207,211,218,0.25) !important; background: transparent !important;
    margin: 6px 0 !important; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}

# file-list div[data-testid="baseButton-secondary"]{ width: 100% !important; }

/* Theme tweaks */
:root{ --page-bg:#0f1116; --card-bg:#1e2229; --card-br:#2a2f37; --muted:#9aa4b2; }
.block-container{ max-width: 1100px; margin:0 auto; padding-top:1rem; padding-bottom:2rem; }
.card{ background: var(--card-bg); border:1px solid var(--card-br); border-radius:14px; padding:18px 20px 16px;
       box-shadow:0 12px 32px rgba(0,0,0,.35); margin-bottom:16px; }
.card .stButton>button{ width:100%; height:40px; border-radius:10px; }
.card label, .card .stMarkdown p { color: var(--muted); }
</style>
""", unsafe_allow_html=True)

# =========================================================
# Session state (router + data)
# =========================================================
if "view" not in st.session_state:
    st.session_state.view = "home"  # "home" | "detail"
if "active_job" not in st.session_state:
    st.session_state.active_job = None  # job_id
if "jobs" not in st.session_state:
    st.session_state.jobs = {}
if "UPLOADED_FILE" not in st.session_state:
    st.session_state.UPLOADED_FILE = None
if "RUNPOD_OBJECT_KEY" not in st.session_state:
    st.session_state.RUNPOD_OBJECT_KEY = None
# editor name default – create BEFORE any widgets
if "editor_name" not in st.session_state:
    st.session_state.editor_name = ""

# =========================================================
# S3 helpers
# =========================================================

# ---------- Unified S3 prefixes ----------
ROOT_PREFIX           = "Srt-model/"
UPLOAD_PREFIX         = f"{ROOT_PREFIX}uploads/"
TRANSCRIPTIONS_PREFIX = f"{ROOT_PREFIX}transcriptions/"
FEEDBACK_PREFIX       = f"{ROOT_PREFIX}feedback/"

def _canonical_path_endpoint(region: str) -> str:
    return f"https://s3api-{region}.runpod.io"

def _dns_ok(host: str) -> bool:
    try:
        socket.gethostbyname(host); return True
    except socket.gaierror:
        return False

UPLOAD_ENDPOINT = _canonical_path_endpoint(S3_REGION)
if not _dns_ok(UPLOAD_ENDPOINT.replace("https://", "")):
    raise RuntimeError(f"DNS cannot resolve {UPLOAD_ENDPOINT}. Check DNS/network.")

s3_upload_client = boto3.client(
    "s3",
    region_name=S3_REGION,
    endpoint_url=UPLOAD_ENDPOINT,
    aws_access_key_id=S3_KEY,
    aws_secret_access_key=S3_SECRET,
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)

def upload_audio_and_get_paths(file_bytes: bytes, original_filename: str) -> Dict[str, Any]:
    guessed = mimetypes.guess_type(original_filename)[0]
    ext = (original_filename.rsplit(".", 1)[-1] if "." in original_filename else "").lower()
    content_type = guessed or ("audio/mpeg" if ext == "mp3" else "audio/wav")
    safe_name = original_filename.replace("/", "_").replace("\\", "_")
    object_key = f"{UPLOAD_PREFIX}{uuid.uuid4()}_{safe_name}"

    try:
        s3_upload_client.put_object(
            Bucket=S3_BUCKET, Key=object_key, Body=file_bytes, ContentType=content_type
        )
    except EndpointConnectionError as e:
        raise RuntimeError(f"Could not reach RunPod S3 endpoint {UPLOAD_ENDPOINT}: {e}")
    except ClientError as e:
        raise RuntimeError(f"S3 upload failed: {e}")
    except Exception as e:
        raise RuntimeError(f"S3 upload error: {e}")
    return {
        "bucket": S3_BUCKET,
        "object_key": object_key,
        "s3_uri": f"s3://{S3_BUCKET}/{object_key}",
        "content_type": content_type,
        "region": S3_REGION,
    }

# ---------- Save transcription assets ----------
def _slugify_name(name: str) -> str:
    base = os.path.splitext(name)[0]
    base = "".join(c if c.isalnum() or c in ("-", "_") else "-" for c in base)
    return base.strip("-_") or f"file-{uuid.uuid4().hex[:8]}"

def _slugify_user(name: str) -> str:
    base = "".join(c if c.isalnum() or c in ("-", "_") else "-" for c in (name or "user"))
    return base.strip("-_") or f"user-{uuid.uuid4().hex[:6]}"

def save_transcription_assets(
    job_id: str,
    filename: str,
    job_record: Dict[str, Any],
    output: Dict[str, Any],
    options: Dict[str, Any],
) -> Dict[str, str]:
    """
    Saves SRT/TXT + meta/output JSON to:
      transcriptions/{slug}_{job_id}/
    Returns dict of s3:// URIs that were written.
    """
    written = {}
    slug = _slugify_name(filename)
    base_dir = f"{TRANSCRIPTIONS_PREFIX}{slug}_{job_id}"

    basename = os.path.splitext(filename)[0]

    
    meta = {
        "job_id": job_id,
        "filename": filename,
        "created_at": job_record.get("created_at"),
        "status": job_record.get("status"),
        "source_bucket": job_record.get("bucket"),
        "source_key": job_record.get("key"),
        "options": options,
        "sizes": {
            "txt_len": len(output.get("txt") or "") if isinstance(output.get("txt"), str) else 0,
            "srt_len": len(output.get("srt") or "") if isinstance(output.get("srt"), str) else 0,
        },
    }
    meta_key = f"{base_dir}/meta.json"
    s3_upload_client.put_object(
        Bucket=S3_BUCKET, Key=meta_key, Body=json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json"
    )
    written["meta"] = f"s3://{S3_BUCKET}/{meta_key}"


    out_key = f"{base_dir}/output.json"
    s3_upload_client.put_object(
        Bucket=S3_BUCKET, Key=out_key, Body=json.dumps(output or {}, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json"
    )
    written["output_json"] = f"s3://{S3_BUCKET}/{out_key}"


    if isinstance(output.get("srt"), str) and output["srt"].strip():
        srt_key = f"{base_dir}/{basename}.srt"
        s3_upload_client.put_object(
            Bucket=S3_BUCKET, Key=srt_key, Body=output["srt"].encode("utf-8"),
            ContentType="text/plain; charset=utf-8"
        )
        written["srt"] = f"s3://{S3_BUCKET}/{srt_key}"

    
    if isinstance(output.get("txt"), str) and output["txt"].strip():
        txt_key = f"{base_dir}/{basename}.txt"
        s3_upload_client.put_object(
            Bucket=S3_BUCKET, Key=txt_key, Body=output["txt"].encode("utf-8"),
            ContentType="text/plain; charset=utf-8"
        )
        written["txt"] = f"s3://{S3_BUCKET}/{txt_key}"

    return written

def save_feedback_to_s3(filename: str, user_name: str, feedback: str, job_id: Optional[str] = None) -> str:
    """
    Stores feedback as JSON to: feedback/<slug(filename)>-<slug(username)>
    Returns the s3:// URI that was written.
    """
    fname_slug = _slugify_name(filename)
    user_slug  = _slugify_user(user_name)
    key = f"{FEEDBACK_PREFIX}{fname_slug}-{user_slug}"

    body = {
        "job_id": job_id,
        "filename": filename,
        "username": user_name,
        "feedback": feedback,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    s3_upload_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8"
    )
    return f"s3://{S3_BUCKET}/{key}"

# ---------- Bootstrap existing transcriptions from S3 ----------
def _safe_json_load(b: bytes) -> dict:
    try:
        return json.loads(b.decode("utf-8"))
    except Exception:
        try:
            return json.loads(b)
        except Exception:
            return {}

def _read_s3_text(bucket: str, key: str) -> Optional[str]:
    try:
        obj = s3_upload_client.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8", errors="ignore")
    except Exception:
        return None

def _read_s3_json(bucket: str, key: str) -> dict:
    try:
        obj = s3_upload_client.get_object(Bucket=bucket, Key=key)
        return _safe_json_load(obj["Body"].read())
    except Exception:
        return {}

def _basename_from_filename(filename: str) -> str:
    return os.path.splitext(os.path.basename(filename))[0]

def _saved_paths_from_base_dir(base_dir: str, filename: str) -> Dict[str, str]:
    basename = _basename_from_filename(filename)
    return {
        "meta":        f"s3://{S3_BUCKET}/{base_dir}/meta.json",
        "output_json": f"s3://{S3_BUCKET}/{base_dir}/output.json",
        "srt":         f"s3://{S3_BUCKET}/{base_dir}/{basename}.srt",
        "txt":         f"s3://{S3_BUCKET}/{base_dir}/{basename}.txt",
    }

def _key_from_uri(uri: str) -> str:
    return uri.split("/", 3)[-1] if uri.startswith("s3://") else uri

def list_existing_transcriptions(limit: int = 1000) -> None:
    """
    Scans both new and legacy locations for transcriptions and seeds session state.
    New:  Srt-model/transcriptions/**/meta.json
    Old:  transcriptions/**/meta.json
    """
    prefixes_to_scan = [TRANSCRIPTIONS_PREFIX, "transcriptions/"]  
    paginator = s3_upload_client.get_paginator("list_objects_v2")
    seen = 0

    for prefix in prefixes_to_scan:
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith("/meta.json"):
                    continue

                base_dir = key.rsplit("/", 1)[0]
                meta = _read_s3_json(S3_BUCKET, key)
                if not meta:
                    continue

                job_id   = str(meta.get("job_id") or base_dir.rsplit("_", 1)[-1])
                filename = meta.get("filename") or base_dir.split("/")[-1]
                status   = meta.get("status", "COMPLETED")
                created_at = meta.get("created_at") or (obj.get("LastModified").timestamp() if obj.get("LastModified") else time.time())

                source_bucket = meta.get("source_bucket", S3_BUCKET)
                source_key    = meta.get("source_key", "")

                saved_paths = _saved_paths_from_base_dir(base_dir, filename)

                st.session_state.jobs[job_id] = {
                    "filename": filename,
                    "bucket": source_bucket,
                    "key": source_key,
                    "status": status,
                    "output": None,
                    "created_at": created_at,
                    "saved_paths": saved_paths,
                }

                seen += 1
                if seen >= limit:
                    return

# =========================================================
# RunPod helpers
# =========================================================
def submit_job(job_input: Dict[str, Any]) -> Dict[str, Any]:
    if not RUN_URL or not RUNPOD_API_KEY:
        raise RuntimeError("Missing RUNPOD endpoint or API key.")
    body = {"input": job_input}
    r = requests.post(RUN_URL, headers=DEFAULT_HEADERS(RUNPOD_API_KEY), data=json.dumps(body), timeout=60)
    r.raise_for_status()
    return r.json()

def poll_status(job_id: str, status_placeholder, poll_interval: float = 2.0, max_wait_sec: int = 1200):
    """(kept for reference; not used in the fast flow)"""
    url = STATUS_URL_BASE + job_id
    start = time.time()
    last_status = None
    with st.spinner("Processing transcription..."):
        while True:
            r = requests.get(url, headers=DEFAULT_HEADERS(RUNPOD_API_KEY), timeout=60)
            r.raise_for_status()
            data = r.json()
            status = data.get("status")
            if status != last_status:
                last_status = status
            if status in ("COMPLETED", "FAILED", "CANCELLED"):
                return data
            if time.time() - start > max_wait_sec:
                raise TimeoutError("Status polling timed out.")
            time.sleep(poll_interval)

def record_job(job_id: str, filename: str, bucket: str, key: str):
    st.session_state.jobs[job_id] = {
        "filename": filename,
        "bucket": bucket,
        "key": key,
        "status": "QUEUED",
        "output": None,
        "created_at": time.time(),
        "saved_paths": None,
        "pending_options": None, 
    }

def update_job(job_id: str, status: str, output: Optional[dict]):
    if job_id in st.session_state.jobs:
        st.session_state.jobs[job_id]["status"] = status
        st.session_state.jobs[job_id]["output"] = output

# ---------- NEW: ping status once per render ----------
def refresh_status_once(job_id: str) -> Optional[dict]:
    try:
        url = STATUS_URL_BASE + job_id
        r = requests.get(url, headers=DEFAULT_HEADERS(RUNPOD_API_KEY), timeout=30)
        r.raise_for_status()
        data = r.json()
        status = data.get("status", "UNKNOWN")
        out = data.get("output") or {}
        update_job(job_id, status, out)

        
        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            job = st.session_state.jobs.get(job_id, {})
            if job and not job.get("saved_paths"):
                options = job.get("pending_options") or {}
                try:
                    saved_paths = save_transcription_assets(
                        job_id=job_id,
                        filename=job.get("filename", f"job_{job_id}"),
                        job_record=job,
                        output=out if isinstance(out, dict) else {},
                        options=options
                    )
                    st.session_state.jobs[job_id]["saved_paths"] = saved_paths
                except Exception as e:
                    st.warning(f"Could not save transcription assets to RunPod S3: {e}")

        return data
    except Exception:
        return None

# ---------- CHANGED: submit → redirect immediately (no blocking) ----------
def run_and_store(payload: Dict[str, Any], filename_for_list: str, ui_area: Optional[st.delta_generator.DeltaGenerator] = None):
    area = ui_area or st
    with st.spinner("Submitting job..."):
        resp = submit_job(payload)

    job_id = resp.get("id") or resp.get("jobId") or resp.get("job_id")
    if not job_id:
        area.warning("No job id found in response.")
        return

    
    record_job(job_id, filename_for_list, payload["bucket"], payload["key"])
    st.session_state.jobs[job_id]["pending_options"] = {
        "language": payload.get("language"),
        "vad_filter": payload.get("vad_filter"),
        "max_words_per_line": payload.get("max_words_per_line"),
        "generate_srt": payload.get("generate_srt"),
        "generate_txt": payload.get("generate_txt"),
        "extension": payload.get("extension"),
        
        "editor_name": st.session_state.get("editor_name", "").strip(),
    }
    st.session_state.active_job = job_id

  
    st.session_state.view = "detail"
    st.rerun()
    return

def _options_for_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Tries to read options (vad_filter, max_words_per_line, etc.)
    from the saved meta.json for this job. Falls back to empty dict.
    """
    try:
        saved = job.get("saved_paths") or {}
        meta_uri = saved.get("meta")
        if not meta_uri:
            return {}
        meta_key = _key_from_uri(meta_uri)   
        meta = _read_s3_json(S3_BUCKET, meta_key)
        if isinstance(meta, dict):
            return meta.get("options") or {}
    except Exception:
        pass
    return {}

# =========================================================
# Bootstrap from S3 on first load
# =========================================================
if "bootstrapped" not in st.session_state:
    try:
        with st.spinner("Loading library from RunPod storage…"):
            list_existing_transcriptions()
    finally:
        st.session_state.bootstrapped = True

# =========================================================
# Swapped Layout: File List in SIDEBAR, Upload+Options on MAIN
# =========================================================
def sidebar_file_library():
    st.sidebar.title("🗂️Transcribed Files History")
    items = sorted(st.session_state.jobs.items(), key=lambda kv: kv[1]["created_at"], reverse=True)

    st.sidebar.markdown('<div id="file-list">', unsafe_allow_html=True)
    for job_id, job in items:
        filename = job["filename"]
        label = filename
        if job.get("status") == "COMPLETED":
            label = f"✅ {filename}"
        elif job.get("status") in {"FAILED", "CANCELLED"}:
            label = f"❌ {filename}"
        if st.sidebar.button(label, key=f"lib_{job_id}", use_container_width=True):
            st.session_state.active_job = job_id
            st.session_state.view = "detail"
            st.rerun()
    st.sidebar.markdown("</div>", unsafe_allow_html=True)

def build_payload(_bucket: str, _key: str, _filename_for_ext: Optional[str] = None) -> Dict[str, Any]:
    ext_from_name = ""
    if _filename_for_ext:
        ext_from_name = os.path.splitext(_filename_for_ext)[1].lstrip(".").lower()
    if not ext_from_name:
        ext_from_name = os.path.splitext(_key)[1].lstrip(".").lower()
    extension = "wav" if ext_from_name == "wav" else "mp3"

    vad_val = bool(st.session_state.get("vad_filter_main", True))
    words_val = int(st.session_state.get("max_words_per_line_main", 7))

    return {
        "bucket": _bucket,
        "key": _key,
        "extension": extension,
        "language": "en",
        "vad_filter": vad_val,
        "max_words_per_line": words_val,
        "generate_srt": "True",
        "generate_txt": "True",
    }

def home_main_upload_area():
    st.markdown("<br>", unsafe_allow_html=True)
    st.title("🎙️ Upload & Transcribe")

    uploaded = st.file_uploader("Upload .mp3 or .wav", type=["mp3", "wav"], key="audio_uploader_main")
    if uploaded is not None:
        file_bytes = uploaded.read()
        st.session_state.UPLOADED_FILE = (file_bytes, uploaded.name)
        st.audio(
            io.BytesIO(file_bytes),
            format="audio/wav" if uploaded.name.lower().endswith(".wav") else "audio/mp3"
        )

    
    st.subheader("Editor")
    editor_name = st.text_input(
        "Editor name (required)",
        value=st.session_state.get("editor_name", ""),
        key="editor_name"
    )

    st.subheader("Transcription Options")
    st.selectbox("VAD filter", [False, True], index=1, key="vad_filter_main")
    st.number_input("Max words per SRT line", min_value=3, max_value=12, value=7, step=1, key="max_words_per_line_main")

    
    name_ok = bool((editor_name or "").strip())
    can_upload = (st.session_state.UPLOADED_FILE is not None) and name_ok

    if not name_ok:
        st.caption("⚠️ Please enter the editor name to enable transcription.")

    work_area = st.container()

    colA, colB = st.columns(2)
    with colA:
        if st.button("⬆️ Upload & Transcribe", use_container_width=True, disabled=not can_upload, key="btn_upload_transcribe_main"):
            if not can_upload:
                if not name_ok:
                    st.error("Editor name is required.")
                else:
                    st.error("Please choose a file above.")
            else:
                try:
                    file_bytes, filename = st.session_state.UPLOADED_FILE
                    st.markdown("<br><br>", unsafe_allow_html=True)
                    with st.spinner("Uploading to RunPod S3…"):
                        result = upload_audio_and_get_paths(file_bytes, filename)

                    st.session_state.RUNPOD_OBJECT_KEY = result["object_key"]
                    payload = build_payload(S3_BUCKET, result["object_key"], _filename_for_ext=filename)

                    
                    run_and_store(payload, filename_for_list=filename, ui_area=work_area)

                except requests.HTTPError as e:
                    work_area.error(f"HTTP error: {e}\n\n{e.response.text}" if getattr(e, "response", None) else str(e))
                except Exception as e:
                    work_area.error(str(e))

    with colB:
        manual_key = st.session_state.RUNPOD_OBJECT_KEY
        
        if st.button("♻️ Regenerate Transcript", use_container_width=True, disabled=not (bool(manual_key) and name_ok), key="btn_regen_main"):
            try:
                if not name_ok:
                    work_area.error("Editor name is required.")
                elif manual_key:
                    filename = (st.session_state.UPLOADED_FILE[1] if st.session_state.UPLOADED_FILE else os.path.basename(manual_key))
                    payload = build_payload(S3_BUCKET, manual_key, _filename_for_ext=filename)
                    with st.spinner("Submitting job..."):
                        pass
                    run_and_store(payload, filename_for_list=filename, ui_area=work_area)
                else:
                    work_area.error("No uploaded object key found. Upload first.")
            except requests.HTTPError as e:
                work_area.error(f"HTTP error: {e}\n\n{e.response.text}" if getattr(e, "response", None) else str(e))
            except Exception as e:
                work_area.error(str(e))

def details_main_area():

    def show_fullscreen_spinner():
        st.markdown(
            """
            <style>
            ._overlay_ {
              position: fixed; inset: 0;
              background: transparent;
              z-index: 9999;
            }
            ._overlay_ ._spinner_ {
              position: absolute; top: 50%; left: 50%;
              transform: translate(-50%, -50%);
              width: 64px; height: 64px;
              border: 6px solid rgba(255,255,255,0.25);
              border-top-color: #fff;
              border-radius: 50%;
              animation: _spin_ 1s linear infinite;
            }
            @keyframes _spin_ { to { transform: translate(-50%, -50%) rotate(360deg); } }
            </style>
            <div class="_overlay_"><div class="_spinner_"></div></div>
            """,
            unsafe_allow_html=True,
        )

    job_id = st.session_state.active_job
    if not job_id or job_id not in st.session_state.jobs:
        st.warning("No job selected. Go back to Home.")
        return

    job = st.session_state.jobs[job_id]
    filename = job["filename"]
    status = job["status"]
    out = job.get("output") or {}

    
    if not out:
        saved = job.get("saved_paths") or {}
        out_uri = saved.get("output_json")
        if out_uri:
            out_key = _key_from_uri(out_uri)
            loaded = _read_s3_json(S3_BUCKET, out_key)
            if loaded:
                out = loaded
                st.session_state.jobs[job_id]["output"] = out  

    st.markdown("<br>", unsafe_allow_html=True)

    col1, col2 = st.columns([0.07, 0.93])
    with col1:
        st.markdown(
            """
            <style>
            .back-btn {
                background-color: #2e3b4e;
                color: white;
                border: none;
                border-radius: 8px;
                font-size: 1.2rem;
                cursor: pointer;
                padding: 6px 10px;
                transition: background-color 0.2s ease;
            }
            .back-btn:hover { background-color: #3f5068; }
            </style>
            """,
            unsafe_allow_html=True
        )
        if st.button("←", key="back_to_home", help="Back to Home", use_container_width=True):
            st.session_state.view = "home"
            st.rerun()

    with col2:
        st.markdown(f"<h1 style='margin: 0; padding: 0;'>📄 {filename}</h1>", unsafe_allow_html=True)

    
    _ = refresh_status_once(job_id)
    status = st.session_state.jobs[job_id].get("status", status)
    st.caption(f"Status: {status}")

    if status not in ("COMPLETED", "FAILED", "CANCELLED"):
        show_fullscreen_spinner()
        time.sleep(2)   
        st.rerun()
        return

    
    if not out:
        saved = job.get("saved_paths") or {}
        out_uri = saved.get("output_json")
        if out_uri:
            out_key = _key_from_uri(out_uri)
            loaded = _read_s3_json(S3_BUCKET, out_key)
            if loaded:
                out = loaded
                st.session_state.jobs[job_id]["output"] = out

    opts = job.get("pending_options") or _options_for_job(job) or {}
    vad_used    = opts.get("vad_filter", None)
    words_used  = opts.get("max_words_per_line", None)
    editor_name = (opts.get("editor_name") or "").strip()

    c1, c2, c3 = st.columns(3)
    with c1:
        st.caption(f"VAD filter: **{'On' if vad_used else 'Off' if vad_used is not None else '—'}**")
    with c2:
        st.caption(f"Words per line: **{words_used if words_used is not None else '—'}**")
    with c3:
        st.caption(f"Editor: **{editor_name or '—'}**")

    st.markdown("<br>", unsafe_allow_html=True)

    if out.get("txt"):
        with st.container():
            st.subheader("📝 Full Transcript (TXT)")
            txt_content = out["txt"]
            num_lines = txt_content.count("\n") + 1
            line_height = 20
            desired_height = min(2000, max(220, num_lines * line_height))
            st.text_area("TXT preview", value=txt_content, height=desired_height)
            st.download_button("⬇️ Download TXT", data=txt_content, file_name=f"{os.path.splitext(filename)[0]}.txt", mime="text/plain")
    if out.get("srt"):
        with st.container():
            st.subheader("📝 Transcript (SRT)")
            srt_content = out["srt"]
            num_lines = srt_content.count("\n") + 1
            line_height = 20
            desired_height = min(2000, max(220, num_lines * line_height))
            st.text_area("SRT preview", value=srt_content, height=desired_height)
            st.download_button("⬇️ Download SRT", data=srt_content, file_name=f"{os.path.splitext(filename)[0]}.srt", mime="text/plain")

    # =========================
    # 💬 Feedback Form
    # =========================
    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("💬 Leave Feedback")

  

    default_fb_name = (opts.get("editor_name") or st.session_state.get("editor_name") or "").strip()

    with st.form("feedback_form", clear_on_submit=True):
        st.text_input("Audio file name", value=filename, disabled=True)
       
        fb_user = st.text_input(
            "Your name",
            value=default_fb_name,
            key=f"fb_name_{job_id}",
            placeholder="Enter your name",
            max_chars=80,
        )
        fb_text = st.text_area("Feedback", placeholder="Write your feedback here...", height=140)
        submitted = st.form_submit_button("Submit Feedback")


    if submitted:
        if not fb_user.strip():
            st.warning("Please enter your name before submitting.")
        elif not fb_text.strip():
            st.warning("Please write some feedback before submitting.")
        else:
            try:
                _ = save_feedback_to_s3(filename=filename, user_name=fb_user.strip(), feedback=fb_text.strip(), job_id=job_id)
                st.success("Thanks! Your feedback was saved")
            except Exception as e:
                st.error(f"Could not save feedback: {e}")

    st.divider()
    cols = st.columns(2)
    if cols[0].button("🏠 Back to Home", use_container_width=True):
        st.session_state.view = "home"
        st.rerun()
    if cols[1].button("🔁 Refresh this page", use_container_width=True):
        st.rerun()


# # =========================================================
# # Pages (Detail page unchanged visually)
# # =========================================================
def page_home():
    sidebar_file_library()
    home_main_upload_area()

def page_detail():
    sidebar_file_library()
    details_main_area()

# =========================================================
# Router
# =========================================================
if st.session_state.view == "home":
    page_home()
else:
    page_detail()
