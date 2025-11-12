import streamlit as st
st.set_page_config(layout="wide", page_title="Video Moderation Tool")
st.title("📹 Video Moderation Tool")

from google.cloud import storage, bigquery
from datetime import datetime, timedelta
import json, os
from google.oauth2 import service_account

# --- Configuration ---
BUCKET_NAME = "galeria-retail-api-dev-moving-images"
BIGQUERY_TABLE = "galeria-retail-api-dev.moving_images.overview"
SERVICE_ACCOUNT_EMAIL = "video-moderator-galeria-retail@galeria-retail-api-dev.iam.gserviceaccount.com"

@st.cache_resource
def get_gcp_clients():
    """Initializes GCP clients using secret-based credentials or default token."""
    try:
        key_json = os.getenv("SERVICE_ACCOUNT_KEY_JSON")
        if key_json:
            credentials_info = json.loads(key_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            storage_client = storage.Client(credentials=credentials)
            bigquery_client = bigquery.Client(credentials=credentials)
        else:
            # fallback to ADC (uses Cloud Run service account)
            storage_client = storage.Client()
            bigquery_client = bigquery.Client()
        return storage_client, bigquery_client
    except Exception as e:
        st.error(f"Failed to initialize GCP clients: {e}")
        st.stop()


storage_client, bq_client = get_gcp_clients()
bucket = storage_client.bucket(BUCKET_NAME)

# --- Configuration for multiple video folders ---
VIDEO_PREFIXES = [
    "output_videos/models/"  #updated storage path
]

@st.cache_data(ttl=300)
def get_processed_videos():
    """Queries BigQuery for videos already processed."""
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
    """Lists all .webp and .mp4 files in the defined prefixes."""
    all_files = []
    for prefix in VIDEO_PREFIXES:
        blobs = bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            # ✅ ADAPTATION #3: Include .mp4 in addition to .webp
            if blob.name.endswith(('.webp', '.mp4')):
                all_files.append(blob.name)
    return all_files


def get_videos_to_review():
    processed = get_processed_videos()
    all_videos = get_all_videos_in_bucket()
    pending = [v for v in all_videos if v not in processed and v not in VIDEO_PREFIXES]
    return pending


# --- Streamlit UI ---
st.title("📹 Video Moderation Tool")
if 'video_queue' not in st.session_state:
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
            service_account_email=SERVICE_ACCOUNT_EMAIL
        )
    except Exception as e:
        st.error(f"Failed to get video blob or URL for '{current_video_name}'. {e}")
        st.stop()

    # Horizontal layout (video left, actions right)
    left_col, right_col = st.columns([0.3, 0.7])

    with left_col:
        if current_video_name.endswith('.webp'):
            st.image(signed_url, use_container_width=True)
        else:
            st.video(signed_url)

    with right_col:
        st.subheader("Video Details")
        st.markdown(f"**Video ID:**\n`{video_id}`")
        st.info(f"**{len(st.session_state.video_queue)}** videos remaining.")
        st.markdown("---")

        notes = st.text_area("Notes", placeholder="Add comments about this video...", height=150)

        def log_decision(decision):
            """Writes moderator’s decision to BigQuery."""
            try:
                moderator_email = "cloud_run_user"
                row_to_insert = [{
                    "video_id": video_id,
                    "decision": decision,
                    "notes": notes,
                    "log_timestamp": datetime.utcnow().isoformat(),
                    "moderator_id": moderator_email
                }]
                errors = bq_client.insert_rows_json(BIGQUERY_TABLE, row_to_insert)
                if not errors:
                    st.toast(f"✅ Decision '{decision}' logged!")
                    st.session_state.video_queue.pop(0)
                    st.rerun()
                else:
                    st.error(f"Error logging to BigQuery: {errors}")
            except Exception as e:
                st.error(f"An error occurred: {e}")

        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("✅ Approve", use_container_width=True):
                log_decision("approve")
        with col2:
            if st.button("♻️ Regenerate", use_container_width=True):
                log_decision("regenerate")
        with col3:
            if st.button("🗑️ Remove", use_container_width=True):
                log_decision("remove")
