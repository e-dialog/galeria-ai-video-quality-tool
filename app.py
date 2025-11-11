import streamlit as st
import json
import os
from google.oauth2 import service_account
from google.cloud import storage, bigquery
from datetime import datetime, timedelta

st.set_page_config(layout="centered", page_title="Video Moderation Tool")

BUCKET_NAME = "galeria-retail-api-dev-moving-images"
BIGQUERY_TABLE = "galeria-retail-api-dev.moving_images.overview"
VIDEO_PREFIXES = ["output_videos/"]

@st.cache_resource
def get_gcp_clients():
    try:
        key_json = os.environ.get("SERVICE_ACCOUNT_KEY_JSON")
        if not key_json:
            st.error("SERVICE_ACCOUNT_KEY_JSON environment variable is missing.")
            st.stop()
        credentials_info = json.loads(key_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        storage_client = storage.Client(credentials=credentials)
        bigquery_client = bigquery.Client(credentials=credentials)
        return storage_client, bigquery_client, credentials
    except Exception as e:
        st.error(f"Failed to initialize GCP clients: {e}")
        st.stop()

storage_client, bq_client, credentials = get_gcp_clients()
bucket = storage_client.bucket(BUCKET_NAME)

@st.cache_data(ttl=300)
def get_processed_videos():
    try:
        query = f"""
            SELECT DISTINCT video_id
            FROM `{BIGQUERY_TABLE}`
            WHERE decision IS NOT NULL
        """
        results = bq_client.query(query).result()
        return {row.video_id for row in results}
    except Exception as e:
        st.error(f"Error querying BigQuery: {e}")
        return set()

@st.cache_data(ttl=300)
def get_all_videos_in_bucket():
    all_files = []
    for prefix in VIDEO_PREFIXES:
        blobs = bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            if blob.name.endswith(".webp"):
                all_files.append(blob.name)
    return all_files

def get_videos_to_review():
    processed = get_processed_videos()
    all_videos = get_all_videos_in_bucket()
    return [v for v in all_videos if v not in processed]

st.title("📹 Video Moderation Tool")

if "video_queue" not in st.session_state:
    st.session_state.video_queue = get_videos_to_review()

if not st.session_state.video_queue:
    st.success("🎉 All videos have been reviewed!")
    if st.button("🔄 Check for New Videos"):
        st.cache_data.clear()
        st.session_state.video_queue = get_videos_to_review()
        st.rerun()
else:
    current_video_name = st.session_state.video_queue[0]
    video_id = current_video_name
    try:
        blob = bucket.blob(current_video_name)
        signed_url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=15),
            service_account_email=credentials.service_account_email,
        )
    except Exception as e:
        st.error(f"Failed to get signed URL: {e}")
        st.stop()
    st.image(signed_url)
    st.subheader("Video Details")
    st.text(f"Video ID: {video_id}")
    st.info(f"{len(st.session_state.video_queue)} videos remaining.")
    notes = st.text_area("🗒️ Notes", placeholder="Add comments about this video...")
    col1, col2, col3 = st.columns(3)

    def log_decision(decision):
        try:
            moderator_email = "cloud_run_user"
            row = [{
                "video_id": video_id,
                "decision": decision,
                "notes": notes,
                "log_timestamp": datetime.utcnow().isoformat(),
                "moderator_id": moderator_email,
            }]
            errors = bq_client.insert_rows_json(BIGQUERY_TABLE, row)
            if not errors:
                st.toast(f"✅ Decision '{decision}' logged!")
                st.session_state.video_queue.pop(0)
                st.rerun()
            else:
                st.error(f"Error logging to BigQuery: {errors}")
        except Exception as e:
            st.error(f"Error writing to BigQuery: {e}")

    if col1.button("✅ Approve"):
        log_decision("approve")
    if col2.button("♻️ Regenerate"):
        log_decision("regenerate")
    if col3.button("🗑️ Remove"):
        log_decision("remove")
