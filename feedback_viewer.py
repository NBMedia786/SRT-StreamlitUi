
import os
import json
import socket
from datetime import datetime
from typing import Optional, List, Dict, Any

import streamlit as st
import pandas as pd
from dotenv import load_dotenv
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError

# =========================
# Secrets loader
# =========================
load_dotenv()

def _sget(key: str, default: Optional[str] = None, section: Optional[str] = None) -> Optional[str]:
    """
    Priority:
      1) Environment variables
      2) st.secrets[section][key] or st.secrets[key]
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
        v2 = st.secrets.get(key, default)
        return v2 if v2 not in ("", None) else default
    except Exception:
        return default

def _require(name: str, value: Optional[str]):
    if not value:
        st.error(f"Missing secret: **{name}**. Add it in `.streamlit/secrets.toml` or App → Settings → Secrets.")
        st.stop()

# S3 (supports grouped [runpod_s3] or flat)
S3_REGION = _sget("RUNPOD_S3_REGION", section="runpod_s3") or _sget("RUNPOD_S3_REGION","eu-ro-1")
S3_BUCKET = _sget("RUNPOD_S3_BUCKET", section="runpod_s3") or _sget("RUNPOD_S3_BUCKET","ul8t514xdg")
S3_KEY    = _sget("RUNPOD_S3_ACCESS_KEY", section="runpod_s3") or _sget("RUNPOD_S3_ACCESS_KEY","user_30aJWkAta5wcvthIEw4jWfUwk0s")
S3_SECRET = _sget("RUNPOD_S3_SECRET_KEY", section="runpod_s3") or _sget("RUNPOD_S3_SECRET_KEY","rps_VJX0XNM36XPGHVU1HE2ET86TAWQCTFQL5QVOIQHQ13uzgb")

for k, v in {
    "RUNPOD_S3_REGION": S3_REGION,
    "RUNPOD_S3_BUCKET": S3_BUCKET,
    "RUNPOD_S3_ACCESS_KEY": S3_KEY,
    "RUNPOD_S3_SECRET_KEY": S3_SECRET,
}.items():
    _require(k, v)

# =========================
# Page + styles
# =========================
st.set_page_config(page_title="Feedback Viewer", page_icon="💬", layout="wide")
st.markdown("""
<style>
:root{ --page-bg:#0f1116; --card-bg:#1e2229; --card-br:#2a2f37; --muted:#9aa4b2; }
.block-container{ max-width: 1200px; margin:0 auto; padding-top:1rem; padding-bottom:2rem; }
.card{ background: var(--card-bg); border:1px solid var(--card-br); border-radius:14px; padding:18px 20px 16px;
       box-shadow:0 12px 32px rgba(0,0,0,.35); margin-bottom:16px; }
.card label, .card .stMarkdown p, .stCaption, .st-emotion-cache-16idsys { color: var(--muted); }
</style>
""", unsafe_allow_html=True)

# =========================
# S3 Client
# =========================
def _dns_ok(host: str) -> bool:
    try:
        socket.gethostbyname(host)
        return True
    except socket.gaierror:
        return False

def _canonical_path_endpoint(region: str) -> str:
    return f"https://s3api-{region}.runpod.io"

ENDPOINT = _canonical_path_endpoint(S3_REGION)
if not _dns_ok(ENDPOINT.replace("https://", "")):
    st.error(f"DNS cannot resolve {ENDPOINT}. Check DNS/network.")
    st.stop()

s3 = boto3.client(
    "s3",
    region_name=S3_REGION,
    endpoint_url=ENDPOINT,
    aws_access_key_id=S3_KEY,
    aws_secret_access_key=S3_SECRET,
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)

# =========================
# Data loaders
# =========================
FEEDBACK_PREFIX = "feedback/"

def _safe_json_load(b: bytes) -> Dict[str, Any]:
    try:
        return json.loads(b.decode("utf-8"))
    except Exception:
        try:
            return json.loads(b)
        except Exception:
            return {}

def list_feedback_keys(limit: int = 5000) -> List[str]:
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=FEEDBACK_PREFIX):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # each feedback saved at: feedback/<slug(filename)>-<slug(username)>
                if key.endswith("/"):  # skip any folder objects
                    continue
                keys.append(key)
                if len(keys) >= limit:
                    return keys
    except ClientError as e:
        st.error(f"Error listing feedback: {e}")
    return keys

def fetch_feedback_record(key: str) -> Optional[Dict[str, Any]]:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = obj["Body"].read()
        data = _safe_json_load(body)
        # ensure fields exist
        return {
            "s3_key": key,
            "filename": data.get("filename", ""),
            "username": data.get("username", ""),
            "feedback": data.get("feedback", ""),
            "job_id": data.get("job_id", ""),
            "created_at": data.get("created_at", obj.get("LastModified").isoformat() if obj.get("LastModified") else None),
        }
    except ClientError as e:
        st.warning(f"Could not read {key}: {e}")
    except Exception as e:
        st.warning(f"Failed parsing {key}: {e}")
    return None

@st.cache_data(ttl=60)
def load_all_feedback() -> pd.DataFrame:
    keys = list_feedback_keys()
    rows = []
    for k in keys:
        rec = fetch_feedback_record(k)
        if rec:
            rows.append(rec)
    if not rows:
        return pd.DataFrame(columns=["filename", "username", "feedback", "job_id", "created_at", "s3_key"])
    df = pd.DataFrame(rows)
    # parse time for sorting
    if "created_at" in df.columns:
        df["created_at_parsed"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    return df

# =========================
# UI
# =========================
st.title("💬 Feedback Viewer")
# st.caption(f"Bucket: `{S3_BUCKET}` • Region: `{S3_REGION}` • Endpoint: `{ENDPOINT}`")

with st.sidebar:
    st.header("Filters")
    refresh = st.button("🔄 Refresh")
    filename_filter = st.text_input("Filename contains", "")
    user_filter = st.text_input("Username contains", "")
    text_filter = st.text_input("Feedback contains", "")
    sort_by = st.selectbox("Sort by", ["created_at_parsed", "filename", "username"])
    sort_asc = st.checkbox("Ascending", value=False)
    max_rows = st.number_input("Max rows to display", min_value=50, max_value=5000, value=1000, step=50)

if refresh:
    load_all_feedback.clear()

with st.container():
    with st.spinner("Loading feedback from RunPod S3…"):
        df = load_all_feedback()

    if df.empty:
        st.info("No feedback found yet under `feedback/`.")
        st.stop()

    # Apply filters
    mask = pd.Series([True] * len(df))
    if filename_filter.strip():
        mask &= df["filename"].fillna("").str.contains(filename_filter.strip(), case=False, na=False)
    if user_filter.strip():
        mask &= df["username"].fillna("").str.contains(user_filter.strip(), case=False, na=False)
    if text_filter.strip():
        mask &= df["feedback"].fillna("").str.contains(text_filter.strip(), case=False, na=False)

    df_view = df[mask].copy()

    # Sort
    if sort_by in df_view.columns:
        df_view = df_view.sort_values(by=sort_by, ascending=sort_asc, na_position="last")

    # Limit
    df_view = df_view.head(int(max_rows))

    # Show summary cards
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("Total feedback", len(df))
    # with c2: st.metric("After filters", len(df_view))
    # with c3: st.metric("Unique files", df_view["filename"].nunique())
    # with c4: st.metric("Unique users", df_view["username"].nunique())

    st.markdown("### Table")
    show_cols = ["created_at_parsed", "filename", "username", "feedback", "job_id", "s3_key"]
    present_cols = [c for c in show_cols if c in df_view.columns]
    st.dataframe(
        df_view[present_cols].rename(columns={"created_at_parsed": "created_at (UTC)"}),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("### Details")
    for _, row in df_view.iterrows():
        with st.expander(f"🗂 {row.get('filename', '')} — 👤 {row.get('username', '')} — 🕒 {row.get('created_at_parsed', '')}"):
            st.write("**S3 key:**", row.get("s3_key", ""))
            st.write("**Job ID:**", row.get("job_id", ""))
            st.write("**Feedback:**")
            st.code(row.get("feedback", ""), language="text")
            # Raw JSON (try to re-read in case the stored file changed)
            try:
                obj = s3.get_object(Bucket=S3_BUCKET, Key=row.get("s3_key"))
                raw = obj["Body"].read().decode("utf-8", errors="ignore")
                st.write("**Raw JSON:**")
                st.code(raw, language="json")
            except Exception as e:
                st.warning(f"Could not fetch raw JSON: {e}")

    # Exports
    # =========================
# Export
# =========================
st.markdown("### Export")

export_cols = ["created_at_parsed", "filename", "username", "feedback", "job_id", "s3_key"]
present_cols = [c for c in export_cols if c in df_view.columns]
export_df = df_view[present_cols].copy()

# Rename and serialize timestamps for export
if "created_at_parsed" in export_df.columns:
    export_df = export_df.rename(columns={"created_at_parsed": "created_at_utc"})
    # Convert pandas Timestamps to ISO strings (safe for JSON/CSV)
    export_df["created_at_utc"] = export_df["created_at_utc"].astype("datetime64[ns, UTC]")
    export_df["created_at_utc"] = export_df["created_at_utc"].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

# CSV
csv_bytes = export_df.to_csv(index=False).encode("utf-8")
st.download_button(
    "⬇️ Download CSV",
    data=csv_bytes,
    file_name="feedback_export.csv",
    mime="text/csv"
)

# JSONL (one JSON object per line)
records = export_df.to_dict(orient="records")
jsonl_str = "\n".join(json.dumps(rec, ensure_ascii=False) for rec in records)
jsonl_bytes = jsonl_str.encode("utf-8")
st.download_button(
    "⬇️ Download JSONL",
    data=jsonl_bytes,
    file_name="feedback_export.jsonl",
    mime="application/json"
)

