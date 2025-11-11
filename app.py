import streamlit as st
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta

# --- Configuration ---
BUCKET_NAME = "galeria-retail-api-dev-moving-images"
BIGQUERY_TABLE = "galeria-retail-api-dev.moving_images.overview"
SERVICE_ACCOUNT_KEY_PATH = "/secrets/video-moderator-key.json"  # secret mount path

VIDEO_PREFIXES = ["output_videos/"]

# --- Initialize GCP Clients ---
@st.cache_resource
def get_gcp_clients():
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_KEY_PATH
        )
        storage_client = storage.Client(credentials=credentials)
        bigquery_client = bigquery.Client(credentials=credentials)
        return storage_client, bigquery_client, credentials
    except Exception as e:
        st.error(f"Failed to initialize GCP clients: {e}")
        st.stop()

storage_client, bq_client, credentials = get_gcp_clients()
bucket = storage_client.bucket(BUCKET_NAME)

# --- Data Fetching ---
@st.cache_data(ttl=300)
def get_processed_videos():
    """Get videos already reviewed."""
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
    """List all .webp files in GCS bucket."""
    all_file_names = []
    for prefix in VIDEO_PREFIXES:
        blobs = bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            if blob.name.endswith(".webp"):
                all_file_names.append(blob.name)
    return all_file_names

def get_videos_to_review():
    """Filter videos not yet processed."""
    processed_video_ids = get_processed_videos()
    all_video_names = get_all_videos_in_bucket()
    return [v for v in all_video_names if v not in processed_video_ids]

# --- UI Setup ---
st.set_page_config(layout="centered", page_title="Video Moderation Tool")
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
        current_blob = bucket.blob(current_video_name)
        signed_url = current_blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=15),
            service_account_email=credentials.service_account_email,
        )
    except Exception as e:
        st.error(
            f"❌ Failed to get video blob or URL for '{current_video_name}'. Check permissions. {e}"
        )
        st.stop()

    # --- UI Display ---
    st.image(signed_url)
    st.subheader("Video Details")
    st.text(f"Video ID: {video_id}")
    st.info(f"{len(st.session_state.video_queue)} videos remaining in the queue.")

    notes = st.text_area("🗒️ Notes", placeholder="Add any comments about this video...")

    # --- Action Buttons ---
    col1, col2, col3 = st.columns(3)

    def log_decision(decision):
        """Write decision to BigQuery."""
        try:
            moderator_email = "cloud_run_user"
            row = [
                {
                    "video_id": video_id,
                    "decision": decision,
                    "notes": notes,
                    "log_timestamp": datetime.utcnow().isoformat(),
                    "moderator_id": moderator_email,
                }
            ]
            errors = bq_client.insert_rows_json(BIGQUERY_TABLE, row)
            if not errors:
                st.toast(f"✅ Decision '{decision}' logged!")
                st.session_state.video_queue.pop(0)
                st.rerun()
            else:
                st.error(f"Error logging to BigQuery: {errors}")
        except Exception as e:
            st.error(f"An error occurred while logging decision: {e}")

    with col1:
        if st.button("✅ Approve", type="primary"):
            log_decision("approve")

    with col2:
        if st.button("♻️ Regenerate"):
            log_decision("regenerate")

    with col3:
        if st.button("🗑️ Remove", type="secondary"):
            log_decision("remove")
