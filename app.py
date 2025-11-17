import streamlit as st
st.set_page_config(layout="wide", page_title="Video Moderation Tool")

from google.cloud import storage, bigquery
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
from google.oauth2 import service_account

# --- Configuration ---
BUCKET_NAME = "galeria-retail-api-dev-moving-images"
BIGQUERY_TABLE = "galeria-retail-api-dev.moving_images.overview"
SERVICE_ACCOUNT_EMAIL = "video-moderator-galeria-retail@galeria-retail-api-dev.iam.gserviceaccount.com"

# --- GCS Locations ---
GCS_INPUT_PREFIX = "input_images_filterded_sorted/"
VIDEO_OUTPUT_PREFIXES = [
    "output_videos/models/",
    "output_videos/generated_models/"
]


# --- Default Prompts ---
DEFAULT_MODEL_PROMPT = """
A hyperrealistic, commercial e-commerce video with a minimalist, gallery aesthetic.
**Subject:** The video is focused entirely on a fashion garment, which is displayed by a figure. The primary subject is the garment itself, emphasizing its fabric texture, design, and silhouette. 
**Environment:** The background MUST be a perfectly clean, flat, shadowless, solid white studio environment. There are no other objects, props, or environmental details.
**Motion:**
The video begins with the camera holding perfectly still for one second on a static, centered, head-on view of the garment.
The figure then begins a **glacial, almost imperceptible** turn to the right. The entire movement from the front view to the side profile must take **no less than 5 seconds** to complete, ensuring the motion is extremely slow and controlled.
The movement **comes to a complete stop precisely when the garment is viewed from a perfect 20-degree side profile.**
**Director's Note on Motion:** The camera MUST remain completely static, locked in place, with no panning, zooming, or shaking. The figure's turn is designed to reveal ONLY the garment's fit and drape from front to side. The final frame must be a true 20-degree profile view, **no more, no less.** Under no circumstances should any part of the garment's rear side be shown.
**Lighting:** The lighting on the primary product must remain perfectly even, diffuse, and shadowless, consistent with professional studio lighting. The product should not be affected by the environmental lighting or cast any shadows onto the background.
**Material & Texture Focus:** As the figure turns, the studio light must subtly catch and define the texture of the fabric. The goal is to make the material look tangible and high-quality.
**Composition & Framing:** The framing must replicate the original input image's perspective perfectly. The garment is perfectly centered with ample, balanced negative space. There is absolutely **no zooming, reframing, or cropping** of the shot. The frame remains completely static, matching the initial perspective; only the figure moves within it.
**Overall Mood & Tone:** The aesthetic is minimalist, sophisticated, calm, and premium. The final output should feel clean, airy, and trustworthy.
**Strict Constraints:**
- The camera must be static. There is absolutely no movement.
- The figure's turn must be extremely slow and smooth.
- The figure's facial expression must remain neutral and consistent throughout.
- **The back of the clothing must never be shown, rendered, or generated.** The view is strictly limited to the front and front-quarter.
- The video must be a single, uninterrupted shot.
- The white background and lighting must remain constant.
- No black bars above or beside the video
"""
def get_default_prompt(source_gcs_path: str) -> str:
    """Determines the default prompt based on the input path."""
    if "products" in source_gcs_path:
        # return PRODUCT_PROMPT (when you add it)
        return "PRODUCT_PROMPT_PLACEHOLDER"
    return DEFAULT_MODEL_PROMPT


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
            storage_client = storage.Client()
            bigquery_client = bigquery.Client()
        return storage_client, bigquery_client
    except Exception as e:
        st.error(f"Failed to initialize GCP clients: {e}")
        st.stop()

storage_client, bq_client = get_gcp_clients()
bucket = storage_client.bucket(BUCKET_NAME)

# --- Helper 1: Get GTIN ---
def get_gtin_from_path(image_name: str) -> str | None:
    """Extracts the GTIN from an image filename."""
    try:
        image_stem = Path(image_name).stem
        gtin_prefix = "generated_"
        gtin_str = ""
        if image_stem.startswith(gtin_prefix):
            gtin_str = image_stem[len(gtin_prefix):].split('_')[0]
        else:
            gtin_str = image_stem.split('_')[0]
        
        if gtin_str.isdigit():
            return gtin_str
    except IndexError:
        pass
    return None

# --- Helper 2: Scan GCS Inputs (for Sync) ---
@st.cache_data(ttl=300)
def get_gcs_input_images(storage_client, bucket_name, prefix):
    """
    Scans GCS input prefix and returns a dict of
    {image_id: "full_gcs_path"} for all valid images.
    """
    st.write(f"Scanning GCS Input: `gs://{bucket_name}/{prefix}`...")
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    gcs_images = {}
    valid_extensions = ('.png', '.jpg', '.jpeg', '.webp')
    
    for blob in blobs:
        if blob.name.endswith(valid_extensions):
            image_id = os.path.basename(blob.name)
            if image_id: 
                gcs_images[image_id] = f"gs://{bucket_name}/{blob.name}"
                
    st.write(f"Found {len(gcs_images)} input images in GCS.")
    return gcs_images

# --- Helper 3: Scan GCS Outputs (for Sync) ---
@st.cache_data(ttl=300)
def get_gcs_output_videos(storage_client, bucket_name, prefixes):
    """
    Scans GCS output prefixes and returns a dict of
    {video_stem: "full_gcs_path"} for all valid videos.
    """
    st.write("Scanning GCS Output folders...")
    gcs_videos = {}
    valid_extensions = ('.mp4', '.webp')
    
    for prefix in prefixes:
        blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
        for blob in blobs:
            if blob.name.endswith(valid_extensions):
                video_stem = Path(blob.name).stem
                if video_stem:
                    gcs_videos[video_stem] = blob.name 
    
    st.write(f"Found {len(gcs_videos)} generated videos in GCS.")
    return gcs_videos

# --- Helper 4: Get All BQ Rows (for Sync) ---
@st.cache_data(ttl=300)
def get_bq_all_rows(bq_client, table_id):
    """Queries BigQuery for all image_ids and their status."""
    st.write("Querying BigQuery for all existing rows...")
    try:
        query = f"""
            SELECT 
                image_id, 
                generation_status, 
                video_id 
            FROM `{table_id}` 
            WHERE image_id IS NOT NULL
        """
        results = bq_client.query(query).result()
        row_map = {row.image_id: dict(row) for row in results}
        st.write(f"Found {len(row_map)} existing rows in BigQuery.")
        return row_map
    except Exception as e:
        if "image_id" in str(e):
             st.warning("Column 'image_id' not found. Returning empty map.")
             return {}
        st.error(f"Error querying BigQuery for existing rows: {e}")
        return {}

# --- Sync Function ---
def sync_gcs_to_bigquery():
    """
    Compares GCS input/output folders with BigQuery table.
    1. Inserts new 'PENDING' rows from GCS inputs.
    2. Updates 'COMPLETED' status for existing rows found in GCS outputs.
    """
    storage_client, bq_client = get_gcp_clients()
    gcs_input_map = get_gcs_input_images(storage_client, BUCKET_NAME, GCS_INPUT_PREFIX)
    gcs_output_map = get_gcs_output_videos(storage_client, BUCKET_NAME, VIDEO_OUTPUT_PREFIXES)
    bq_rows_map = get_bq_all_rows(bq_client, BIGQUERY_TABLE)
    bq_existing_ids = set(bq_rows_map.keys())
    
    # Task 1: Find new images to add
    new_image_ids = set(gcs_input_map.keys()) - bq_existing_ids
    rows_to_insert = []
    
    if new_image_ids:
        st.write(f"Found {len(new_image_ids)} new images. Preparing to insert...")
        for image_id in new_image_ids:
            source_path = gcs_input_map[image_id]
            gtin = get_gtin_from_path(image_id)
            
            rows_to_insert.append({
                "image_id": image_id,
                "gtin": gtin,
                "source_gcs_path": source_path,
                "generation_status": "PENDING",
                "generation_attempts": 0,
                "last_updated": datetime.utcnow().isoformat(),
                "prompt": get_default_prompt(source_path)
            })
            
        try:
            errors = bq_client.insert_rows_json(BIGQUERY_TABLE, rows_to_insert)
            if errors:
                st.error(f"Error inserting new rows: {errors}")
        except Exception as e:
            st.error(f"An error occurred during BQ insert: {e}")
            
    # Task 2: "Back-fill" - Find existing rows to update
    st.write("Checking for existing videos to sync...")
    updates_to_run = []
    stem_to_image_id_map = {Path(img_id).stem: img_id for img_id in bq_existing_ids}
    
    for video_stem, video_path in gcs_output_map.items():
        if video_stem in stem_to_image_id_map:
            image_id = stem_to_image_id_map[video_stem]
            bq_row = bq_rows_map[image_id]
            
            if bq_row.get('generation_status') == 'PENDING' or bq_row.get('video_id') is None:
                updates_to_run.append((image_id, video_path))

    if updates_to_run:
        st.write(f"Found {len(updates_to_run)} videos to sync. Updating BQ...")
        for image_id, video_path in updates_to_run:
            try:
                query = f"""
                    UPDATE `{BIGQUERY_TABLE}`
                    SET 
                        generation_status = 'COMPLETED',
                        video_id = @video_path,
                        last_updated = @timestamp
                    WHERE image_id = @image_id
                """
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("video_path", "STRING", video_path),
                        bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", datetime.utcnow().isoformat()),
                        bigquery.ScalarQueryParameter("image_id", "STRING", image_id),
                    ]
                )
                bq_client.query(query, job_config=job_config).result()
            except Exception as e:
                st.error(f"Failed to back-fill {image_id}: {e}")
                
    return len(rows_to_insert), len(updates_to_run)


# --- Get Moderation Queue ---
@st.cache_data(ttl=60)
def get_videos_to_review():
    """
    Queries BigQuery for videos that are generated and ready for moderation.
    """
    print("Querying BQ for videos to review...")
    try:
        query = f"""
            SELECT 
                image_id, 
                video_id, 
                prompt,
                notes
            FROM `{BIGQUERY_TABLE}`
            WHERE generation_status = 'COMPLETED'
              AND decision IS NULL
            ORDER BY last_updated ASC
        """
        results = bq_client.query(query).result()
        queue = [dict(row) for row in results]
        return queue
    except Exception as e:
        st.error(f"Error querying BigQuery for review queue: {e}")
        return []

# --- MODIFIED: Log Decision ---
def update_decision_in_bq(moderator_id, image_id, decision, new_prompt, new_notes, video_path_to_delete=None):
    """
    Performs a BigQuery UPDATE to log the moderation decision.
    Now requires moderator_id.
    """
    print(f"Updating decision for {image_id} by {moderator_id}: {decision}")
    
    if decision == 'regenerate' and video_path_to_delete:
        st.write(f"Regenerate requested. Deleting old video: {video_path_to_delete}")
        try:
            blob = bucket.blob(video_path_to_delete)
            blob.delete()
            st.toast(f"Deleted old video: {video_path_to_delete}")
        except Exception as e:
            st.warning(f"Could not delete old video {video_path_to_delete}: {e}")
    
    new_generation_status = 'MODERATED'
    if decision == 'regenerate':
        new_generation_status = 'PENDING'

    # Use the passed-in moderator_id
    moderator_email = moderator_id
    
    query = f"""
        UPDATE `{BIGQUERY_TABLE}`
        SET 
            decision = @decision,
            generation_status = @gen_status,
            prompt = @prompt,
            notes = @notes,
            moderator_id = @moderator,
            log_timestamp = @timestamp,
            last_updated = @timestamp,
            video_id = CASE 
                WHEN @gen_status = 'PENDING' THEN NULL 
                ELSE video_id 
            END
        WHERE image_id = @image_id
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("decision", "STRING", decision),
            bigquery.ScalarQueryParameter("gen_status", "STRING", new_generation_status),
            bigquery.ScalarQueryParameter("prompt", "STRING", new_prompt),
            bigquery.ScalarQueryParameter("notes", "STRING", new_notes),
            bigquery.ScalarQueryParameter("moderator", "STRING", moderator_email), # Use variable
            bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", datetime.utcnow().isoformat()),
            bigquery.ScalarQueryParameter("image_id", "STRING", image_id),
        ]
    )
    
    try:
        bq_client.query(query, job_config=job_config).result()
        st.toast(f"✅ Decision '{decision}' logged for {image_id}!")
        
        st.cache_data.clear()
        st.session_state.video_queue.pop(0)
        st.rerun()
        
    except Exception as e:
        st.error(f"An error occurred updating BigQuery: {e}")


# --- Streamlit UI ---
st.title("📹 Video Moderation Tool")

# --- NEW: Moderator Login Logic ---
if 'moderator_id' not in st.session_state:
    st.sidebar.header("Login")
    moderator_name = st.sidebar.text_input("Please enter your name to begin:", key="moderator_name_input")
    
    if st.sidebar.button("Login"):
        if moderator_name:
            st.session_state.moderator_id = moderator_name
            st.rerun() # Rerun the app to show the main content
        else:
            st.sidebar.error("Name cannot be empty.")
    
    # Lock the main app
    st.info("Please log in using the sidebar to start moderation.")

else:
    # --- APP IS "UNLOCKED" ---
    moderator_id = st.session_state.moderator_id
    st.sidebar.success(f"Logged in as: **{moderator_id}**")
    st.sidebar.header("Admin Tools")
    
    # --- SYNC BUTTON (now inside the 'else') ---
    if st.sidebar.button("🔄 Sync GCS & BigQuery"):
        with st.spinner("Performing 2-way sync..."):
            st.cache_data.clear() 
            new_items, updated_items = sync_gcs_to_bigquery()
            st.cache_data.clear()
            
            st.sidebar.success(f"Sync complete!")
            st.sidebar.json({
                "New images added": new_items,
                "Existing videos synced": updated_items
            })
            st.rerun() 
    # --- END SYNC BUTTON ---

    # --- Main Moderation Logic (now inside the 'else') ---
    if 'video_queue' not in st.session_state:
        st.session_state.video_queue = get_videos_to_review()

    if not st.session_state.video_queue:
        st.success("🎉 All videos have been reviewed!")
        if st.button("🔄 Check for New Videos"):
            st.cache_data.clear()
            st.session_state.video_queue = get_videos_to_review()
            st.rerun()
    else:
        current_video_data = st.session_state.video_queue[0]
        
        image_id = current_video_data["image_id"]
        video_path_gcs = current_video_data["video_id"]
        initial_prompt = current_video_data["prompt"]
        initial_notes = current_video_data["notes"] if current_video_data["notes"] else ""

        if not video_path_gcs:
            st.error(f"Data for {image_id} is corrupted. 'video_id' is missing.")
            st.stop()

        try:
            blob = bucket.blob(video_path_gcs)
            signed_url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(minutes=15),
                service_account_email=SERVICE_ACCOUNT_EMAIL
            )
        except Exception as e:
            st.error(f"Failed to get video blob or URL for '{video_path_gcs}'. {e}")
            st.stop()

        left_col, right_col = st.columns([0.3, 0.7])

        with left_col:
            if video_path_gcs.endswith('.webp'):
                st.image(signed_url, use_container_width=True)
            else:
                st.video(signed_url)

        with right_col:
            st.subheader("Video Details")
            st.markdown(f"**Image ID:**\n`{image_id}`")
            st.markdown(f"**Video Path:**\n`gs://{BUCKET_NAME}/{video_path_gcs}`")
            st.info(f"**{len(st.session_state.video_queue)}** videos remaining.")
            st.markdown("---")

            st.subheader("📝 Prompt")
            edited_prompt = st.text_area(
                "Prompt used for generation (edit here before regenerating):", 
                value=initial_prompt, 
                height=300
            )
            
            st.subheader("📋 Notes")
            edited_notes = st.text_area(
                "Moderation notes:", 
                value=initial_notes,
                placeholder="Add comments about this video...", 
                height=150
            )

            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("✅ Approve", use_container_width=True):
                    # Pass the moderator_id
                    update_decision_in_bq(moderator_id, image_id, "approve", edited_prompt, edited_notes)
            with col2:
                if st.button("♻️ Regenerate", use_container_width=True):
                    # Pass the moderator_id
                    update_decision_in_bq(moderator_id, image_id, "regenerate", edited_prompt, edited_notes, video_path_to_delete=video_path_gcs)
            with col3:
                if st.button("🗑️ Remove", use_container_width=True):
                    # Pass the moderator_id
                    update_decision_in_bq(moderator_id, image_id, "remove", edited_prompt, edited_notes, video_path_to_delete=video_path_gcs)