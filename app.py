import os
import io
import uuid
import socket
import mimetypes
import time
import json
from typing import Dict, Any, Optional

import streamlit as st
from dotenv import load_dotenv
import requests
import boto3
from botocore.client import Config
from botocore.exceptions import EndpointConnectionError, ClientError

# =========================================================
# ENV
# =========================================================
load_dotenv()

S3_REGION = os.getenv("RUNPOD_S3_REGION")
S3_BUCKET = os.getenv("RUNPOD_S3_BUCKET")
S3_KEY    = os.getenv("RUNPOD_S3_ACCESS_KEY")
S3_SECRET = os.getenv("RUNPOD_S3_SECRET_KEY")

if not all([S3_REGION, S3_BUCKET, S3_KEY, S3_SECRET]):
    raise RuntimeError("Missing RUNPOD_S3_REGION / RUNPOD_S3_BUCKET / RUNPOD_S3_ACCESS_KEY / RUNPOD_S3_SECRET_KEY")

RUNPOD_API_KEY   = os.getenv("RUNPOD_API_KEY")
RUNPOD_ENDPOINT  = os.getenv("RUNPOD_ENDPOINT_ID")
RUN_URL          = f"https://api.runpod.ai/v2/{RUNPOD_ENDPOINT}/run" if RUNPOD_ENDPOINT else ""
STATUS_URL_BASE  = f"https://api.runpod.ai/v2/{RUNPOD_ENDPOINT}/status/" if RUNPOD_ENDPOINT else ""
DEFAULT_HEADERS  = lambda key: {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

# =========================================================
# Page + styles (set_page_config must be first Streamlit call)
# =========================================================
st.set_page_config(page_title="SRT Generator ", page_icon="🎧", layout="wide", initial_sidebar_state="expanded",)

st.markdown("""
<style>
/* Hide collapse/expand controls */
button[aria-label="Hide sidebar"],
button[aria-label="Show sidebar"],
[data-testid="collapsedControl"],
[data-testid="stSidebarCollapseButton"] { display: none !important; }
/* Keep sidebar visible */
section[data-testid="stSidebar"] { transform: none !important; visibility: visible !important; opacity: 1 !important; }
@media (max-width: 1200px) { section[data-testid="stSidebar"] { position: sticky !important; left: 0 !important; } }
/* Sidebar width */
section[data-testid="stSidebar"] { min-width: 280px; max-width: 3200px; }
/* File grid buttons */
#file-list button[data-baseweb="button"]{
    height: 70px !important; border-radius: 18px !important; font-weight: 700 !important; font-size: 20px !important;
    letter-spacing: .2px; border: 1px solid rgba(207,211,218,0.25) !important; background: transparent !important;
    margin: 6px 0 !important; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
#file-list div[data-testid="baseButton-secondary"]{ width: 100% !important; }
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

# =========================================================
# S3 helpers
# =========================================================
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
    object_key = f"uploads/{uuid.uuid4()}_{safe_name}"
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
    base_dir = f"transcriptions/{slug}_{job_id}"
    basename = os.path.splitext(filename)[0]

    # Meta (summary)
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

    # Raw output
    out_key = f"{base_dir}/output.json"
    s3_upload_client.put_object(
        Bucket=S3_BUCKET, Key=out_key, Body=json.dumps(output or {}, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json"
    )
    written["output_json"] = f"s3://{S3_BUCKET}/{out_key}"

    # SRT
    if isinstance(output.get("srt"), str) and output["srt"].strip():
        srt_key = f"{base_dir}/{basename}.srt"
        s3_upload_client.put_object(
            Bucket=S3_BUCKET, Key=srt_key, Body=output["srt"].encode("utf-8"),
            ContentType="text/plain; charset=utf-8"
        )
        written["srt"] = f"s3://{S3_BUCKET}/{srt_key}"

    # TXT
    if isinstance(output.get("txt"), str) and output["txt"].strip():
        txt_key = f"{base_dir}/{basename}.txt"
        s3_upload_client.put_object(
            Bucket=S3_BUCKET, Key=txt_key, Body=output["txt"].encode("utf-8"),
            ContentType="text/plain; charset=utf-8"
        )
        written["txt"] = f"s3://{S3_BUCKET}/{txt_key}"

    return written

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
    # uri like s3://bucket/path -> return 'path'
    return uri.split("/", 3)[-1] if uri.startswith("s3://") else uri

def list_existing_transcriptions(limit: int = 1000) -> None:
    """
    Scans s3://{bucket}/transcriptions/**/meta.json and seeds st.session_state.jobs.
    Safe to call multiple times; it updates/merges entries by job_id.
    """
    prefix = "transcriptions/"
    paginator = s3_upload_client.get_paginator("list_objects_v2")
    seen = 0

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/meta.json"):
                continue

            # base_dir like transcriptions/<slug>_<jobid>
            base_dir = key.rsplit("/", 1)[0]
            meta = _read_s3_json(S3_BUCKET, key)
            if not meta:
                continue

            job_id = str(meta.get("job_id") or base_dir.rsplit("_", 1)[-1])
            filename = meta.get("filename") or base_dir.split("/")[-1]
            status = meta.get("status", "COMPLETED")
            # Fallback to object LastModified if created_at not present
            created_at = meta.get("created_at") or (obj.get("LastModified").timestamp() if obj.get("LastModified") else time.time())

            # best-effort source bucket/key for traceability
            source_bucket = meta.get("source_bucket", S3_BUCKET)
            source_key    = meta.get("source_key", "")

            saved_paths = _saved_paths_from_base_dir(base_dir, filename)

            # Seed/merge into jobs
            st.session_state.jobs[job_id] = {
                "filename": filename,
                "bucket": source_bucket,
                "key": source_key,
                "status": status,
                "output": None,           # lazy-load later on detail page
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
        "saved_paths": None,  # will hold S3 URIs if saved
    }

def update_job(job_id: str, status: str, output: Optional[dict]):
    if job_id in st.session_state.jobs:
        st.session_state.jobs[job_id]["status"] = status
        st.session_state.jobs[job_id]["output"] = output

def run_and_store(payload: Dict[str, Any], filename_for_list: str, ui_area: Optional[st.delta_generator.DeltaGenerator] = None):
    """
    ui_area: a container/placeholder on the MAIN PAGE where we want to show progress.
    """
    area = ui_area or st
    status_placeholder = area.empty()

    # Submit job
    with st.spinner("Submitting job..."):
        resp = submit_job(payload)

    job_id = resp.get("id") or resp.get("jobId") or resp.get("job_id")
    if not job_id:
        area.warning("No job id found in response.")
        return

    record_job(job_id, filename_for_list, payload["bucket"], payload["key"])
    st.session_state.active_job = job_id

    # Poll status
    data = poll_status(job_id, status_placeholder)

    status = data.get("status", "UNKNOWN")
    out = (data.get("output") or {}) if isinstance(data, dict) else {}
    if not out and "output" in data:
        out = data["output"]

    update_job(job_id, status, out)

    # Save assets to RunPod S3 (always, so there is a record)
    options = {
        "language": payload.get("language"),
        "vad_filter": payload.get("vad_filter"),
        "max_words_per_line": payload.get("max_words_per_line"),
        "generate_srt": payload.get("generate_srt"),
        "generate_txt": payload.get("generate_txt"),
        "extension": payload.get("extension"),
    }
    try:
        saved_paths = save_transcription_assets(
            job_id=job_id,
            filename=filename_for_list,
            job_record=st.session_state.jobs[job_id],
            output=out if isinstance(out, dict) else {},
            options=options
        )
        st.session_state.jobs[job_id]["saved_paths"] = saved_paths
    except Exception as e:
        area.warning(f"Could not save transcription assets to RunPod S3: {e}")
        saved_paths = None

    

    # Navigate to details
    st.session_state.view = "detail"

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
# Sidebar (ALL inputs + actions)
# =========================================================
st.sidebar.title("🎙️ Upload Audio/Video")

# Upload
uploaded = st.sidebar.file_uploader("Upload .mp3 or .wav", type=["mp3", "wav"], key="audio_uploader")
if uploaded is not None:
    file_bytes = uploaded.read()
    st.session_state.UPLOADED_FILE = (file_bytes, uploaded.name)
    # Audio preview in sidebar
    st.sidebar.audio(
        io.BytesIO(file_bytes),
        format="audio/wav" if uploaded.name.lower().endswith(".wav") else "audio/mp3"
    )

# Options
st.sidebar.subheader("Transcription Options")
vad_filter = st.sidebar.selectbox("VAD filter", [False, True], index=0)
max_words_per_line = st.sidebar.number_input(
    "Max words per SRT line", min_value=3, max_value=12, value=7, step=1
)

def build_payload(_bucket: str, _key: str, _filename_for_ext: Optional[str] = None) -> Dict[str, Any]:
    # Prefer extension from actual filename if provided; else from key
    ext_from_name = ""
    if _filename_for_ext:
        ext_from_name = os.path.splitext(_filename_for_ext)[1].lstrip(".").lower()
    if not ext_from_name:
        ext_from_name = os.path.splitext(_key)[1].lstrip(".").lower()
    extension = "wav" if ext_from_name == "wav" else "mp3"
    return {
        "bucket": _bucket,
        "key": _key,
        "extension": extension,
        "language": "en",
        "vad_filter": bool(vad_filter),
        "max_words_per_line": int(max_words_per_line),
        "generate_srt": "True",
        "generate_txt": "True",
    }

# Buttons
can_upload = st.session_state.UPLOADED_FILE is not None
if st.sidebar.button("⬆️ Upload & Transcribe", use_container_width=True, disabled=not can_upload):
    if not can_upload:
        st.sidebar.error("Please choose a file above.")
    else:
        # Main page container for progress + results
        work_area = st.container()

        try:
            file_bytes, filename = st.session_state.UPLOADED_FILE

            # Show the "Uploading to RunPod S3…" spinner ON THE MAIN PAGE
            st.markdown("<br><br>", unsafe_allow_html=True)
            with st.spinner("Uploading to RunPod S3…"):
                result = upload_audio_and_get_paths(file_bytes, filename)

            st.session_state.RUNPOD_OBJECT_KEY = result["object_key"]
            payload = build_payload(S3_BUCKET, result["object_key"], _filename_for_ext=filename)

            # Kick off the run — progress/status will appear in the same MAIN area
            run_and_store(payload, filename_for_list=filename, ui_area=work_area)

        except requests.HTTPError as e:
            work_area.error(f"HTTP error: {e}\n\n{e.response.text}" if getattr(e, "response", None) else str(e))
        except Exception as e:
            work_area.error(str(e))

# Manual submit (if object key already known from a previous upload)
manual_key = st.session_state.RUNPOD_OBJECT_KEY
if st.sidebar.button("♻️ Regenerate Transcript", use_container_width=True, disabled=not bool(manual_key)):
    work_area = st.container()
    try:
        if manual_key:
            filename = (st.session_state.UPLOADED_FILE[1] if st.session_state.UPLOADED_FILE else os.path.basename(manual_key))
            payload = build_payload(S3_BUCKET, manual_key, _filename_for_ext=filename)

            # Optional tiny spinner (visual cue on main page)
            with st.spinner("Submitting job..."):
                pass

            run_and_store(payload, filename_for_list=filename, ui_area=work_area)
        else:
            work_area.error("No uploaded object key found. Upload first.")
    except requests.HTTPError as e:
        work_area.error(f"HTTP error: {e}\n\n{e.response.text}" if getattr(e, "response", None) else str(e))
    except Exception as e:
        work_area.error(str(e))

# =========================================================
# Pages
# =========================================================
def page_home():
    st.markdown("<br>", unsafe_allow_html=True)
    st.title("🗂️ Transcribed Files")
    st.markdown("Click an audio filename to view its transcription details.\n")

    

    items = sorted(st.session_state.jobs.items(), key=lambda kv: kv[1]["created_at"], reverse=True)

    # Wrap the grid so our CSS targets only these buttons
    st.markdown('<div id="file-list">', unsafe_allow_html=True)

    # Render two buttons per row
    for i in range(0, len(items), 2):
        cols = st.columns(2, gap="large")
        for j in range(2):
            if i + j < len(items):
                job_id, job = items[i + j]
                filename = job["filename"]
                label = filename
                # Optional status glyph
                if job.get("status") == "COMPLETED":
                    label = f"✅ {filename}"
                elif job.get("status") in {"FAILED", "CANCELLED"}:
                    label = f"❌ {filename}"
                with cols[j]:
                    if st.button(label, key=f"btn_{job_id}", use_container_width=True):
                        st.session_state.active_job = job_id
                        st.session_state.view = "detail"
                        st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)

def page_detail():
    job_id = st.session_state.active_job
    if not job_id or job_id not in st.session_state.jobs:
        st.warning("No job selected. Go back to Home.")
        return

    job = st.session_state.jobs[job_id]
    filename = job["filename"]
    status = job["status"]
    out = job.get("output") or {}

    # Lazy-load output.json if needed
    if not out:
        saved = job.get("saved_paths") or {}
        out_uri = saved.get("output_json")
        if out_uri:
            out_key = _key_from_uri(out_uri)
            loaded = _read_s3_json(S3_BUCKET, out_key)
            if loaded:
                out = loaded
                st.session_state.jobs[job_id]["output"] = out  # cache

    # st.title(f"📄 {filename}")
    st.markdown("<br>", unsafe_allow_html=True)
    



    # Back arrow + title in one row
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
            .back-btn:hover {
                background-color: #3f5068;
            }
            </style>
            """,
            unsafe_allow_html=True
        )
        if st.button("←", key="back_to_home", help="Back to Home", use_container_width=True):
            st.session_state.view = "home"
            st.rerun()

    with col2:
        st.markdown(f"<h1 style='margin: 0; padding: 0;'>📄 {filename}</h1>", unsafe_allow_html=True)




   


    st.caption(f"Status: {status}")
    st.markdown("<br>", unsafe_allow_html=True)

    if out.get("txt"):
        with st.container():
            st.subheader("📝 Full Transcript (TXT)")
            txt_content = out["txt"]
            num_lines = txt_content.count("\n") + 1
            line_height = 20  # px per line
            desired_height = min(2000, max(220, num_lines * line_height))
            st.text_area("TXT preview", value=txt_content, height=desired_height)
            st.download_button("⬇️ Download TXT", data=txt_content, file_name=f"{os.path.splitext(filename)[0]}.txt", mime="text/plain")
    if out.get("srt"):
        with st.container():
            st.subheader("📝 Transcript (SRT)")
            srt_content = out["srt"]
            num_lines = srt_content.count("\n") + 1
            line_height = 20  # px per line
            desired_height = min(2000, max(220, num_lines * line_height))
            st.text_area("SRT preview", value=srt_content, height=desired_height)
            st.download_button("⬇️ Download SRT", data=srt_content, file_name=f"{os.path.splitext(filename)[0]}.srt", mime="text/plain")

    

    st.divider()
    cols = st.columns(2)
    if cols[0].button("🏠 Back to Home", use_container_width=True):
        st.session_state.view = "home"
        st.rerun()
    if cols[1].button("🔁 Refresh this page", use_container_width=True):
        st.rerun()

# =========================================================
# Router
# =========================================================
if st.session_state.view == "home":
    page_home()
else:
    page_detail()
