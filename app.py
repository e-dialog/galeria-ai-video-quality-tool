import streamlit as st
from google.cloud import storage, bigquery
from datetime import datetime, timedelta  

# --- Configuration ---
BUCKET_NAME = "galeria-retail-api-dev-moving-images"
VIDEO_PREFIX = "output_videos/"
BIGQUERY_TABLE = "galeria-retail-api-dev.moving_images.overview"
SERVICE_ACCOUNT_EMAIL = "video-moderator-galeria-retail@galeria-retail-api-dev.iam.gserviceaccount.com"

# Initialize clients
@st.cache_resource
def get_gcp_clients():
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()
    return storage_client, bigquery_client

storage_client, bq_client = get_gcp_clients()
bucket = storage_client.bucket(BUCKET_NAME)


@st.cache_data(ttl=300) # Cache for 5 minutes
def get_processed_videos():
    """Queries BigQuery to find videos that already have a decision."""
    try:
        query = f"""
            SELECT DISTINCT video_id
            FROM `{BIGQUERY_TABLE}`
            WHERE decision IS NOT NULL
        """
        query_job = bq_client.query(query)
        results = query_job.result()
        return {row.video_id for row in results}
    except Exception as e:
        st.error(f"Error querying BigQuery: {e}")
        return set()

@st.cache_data(ttl=300) 
def get_all_videos_in_bucket():
    """Lists all video file *names* in the GCS bucket prefix."""
    blobs = bucket.list_blobs(prefix=VIDEO_PREFIX)
    # Return a list of strings (blob names), which are serializable
    
    return [blob.name for blob in blobs if blob.name.endswith(('.mp4', '.mov', '.avi', 'webp'))] 
 

def get_videos_to_review():
    processed_video_ids = get_processed_videos() 
    all_video_names = get_all_videos_in_bucket() 
    
    pending_videos = []
    # Filter the list of strings
    for video_name in all_video_names:
        # Filter out empty strings/folder names
        if video_name == VIDEO_PREFIX or not video_name:
            continue
        if video_name not in processed_video_ids:
            pending_videos.append(video_name)
            
    return pending_videos 


# --- Main App ---
st.set_page_config(layout="centered")
st.title("📹 Video Moderation Tool")

# Initialize session state for the video list
if 'video_queue' not in st.session_state:
    st.session_state.video_queue = get_videos_to_review()

# --- Main App Logic ---
if not st.session_state.video_queue:
    st.success("🎉 All videos have been reviewed!")
    if st.button("Check for New Videos"):
        # Clear the cache and re-fetch
        st.cache_data.clear()
        st.session_state.video_queue = get_videos_to_review()
        st.rerun()
else:
    # Get the next video *name* (which is a string)
    current_video_name = st.session_state.video_queue[0]
    video_id = current_video_name # The name is the ID

    # Get the actual blob object from its name
    try:
        current_blob = bucket.blob(current_video_name) # Get the blob from the bucket
        signed_url = current_blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=15),
            service_account_email=SERVICE_ACCOUNT_EMAIL
        )
    except Exception as e:
        st.error(f"Failed to get video blob or URL for '{current_video_name}'. Check permissions. {e}")
        st.stop()

    # --- UI from Mockup ---
    st.image(signed_url)
    
    st.subheader("Video Details")
    st.text(f"Video ID: {video_id}")
    st.info(f"{len(st.session_state.video_queue)} videos remaining in the queue.")

    notes = st.text_area("Notes")

    # --- Button Actions ---
    col1, col2, col3 = st.columns(3)
    
    def log_decision(decision):
        """Writes the user's decision to BigQuery."""
        try:
            # Safely check for user email, default to 'local_dev'
            moderator_email = "local_dev"
            
            if hasattr(st, 'user') and hasattr(st.user, 'email') and st.user.email:
                 moderator_email = st.user.email
            elif hasattr(st, 'experimental_user') and hasattr(st.experimental_user, 'email') and st.experimental_user.email:
                 moderator_email = st.experimental_user.email
            
            row_to_insert = [{
                "video_id": video_id,
                "decision": decision,
                "notes": notes,
                "log_timestamp": datetime.utcnow().isoformat(),
                "moderator_id": moderator_email
            }]
            
            errors = bq_client.insert_rows_json(BIGQUERY_TABLE, row_to_insert)
            if errors == []:
                st.toast(f"Decision '{decision}' logged!")
                # Success! Remove from queue and rerun
                st.session_state.video_queue.pop(0)
                st.rerun()
            else:
                st.error(f"Error logging to BigQuery: {errors}")
                
        except Exception as e:
            st.error(f"An error occurred: {e}")

    if col1.button("Approve", type="primary"):
        log_decision("approve")

    if col2.button("Regenerate"):
        log_decision("regenerate")

    if col3.button("Remove", type="secondary"):
        log_decision("remove")