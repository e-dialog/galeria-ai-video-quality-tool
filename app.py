import streamlit as st
st.set_page_config(layout="wide", page_title="Video Moderation Tool")

from google.cloud import storage, bigquery
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
from google.oauth2 import service_account
import tempfile
from moviepy import VideoFileClip 

# --- Configuration ---
BUCKET_NAME = "galeria-retail-api-dev-moving-images"
BIGQUERY_TABLE = "galeria-retail-api-dev.moving_images.overview"
SERVICE_ACCOUNT_EMAIL = "video-moderator-galeria-retail@galeria-retail-api-dev.iam.gserviceaccount.com"

# --- GCS Locations ---
GCS_INPUT_PREFIX = "input_images_filterded_sorted/"
GCS_PENDING_FOLDER = "ai-video-quality-tool/output/pending/"
GCS_APPROVED_FOLDER = "ai-video-quality-tool/output/approved/"

# Sync looks here for videos
VIDEO_OUTPUT_PREFIXES = [GCS_PENDING_FOLDER]


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
    if "products" in source_gcs_path:
        return "PRODUCT_PROMPT_PLACEHOLDER"
    return DEFAULT_MODEL_PROMPT

@st.cache_resource
def get_gcp_clients():
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

# --- Helpers ---

def get_gtin_from_path(image_name: str) -> str | None:
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

@st.cache_data(ttl=300)
def get_gcs_input_images(_storage_client, bucket_name, prefix):
    st.write(f"Scanning GCS Input: `gs://{bucket_name}/{prefix}`...")
    blobs = _storage_client.list_blobs(bucket_name, prefix=prefix)
    gcs_images = {}
    valid_extensions = ('.png', '.jpg', '.jpeg', '.webp')
    for blob in blobs:
        if blob.name.endswith(valid_extensions):
            image_id = os.path.basename(blob.name)
            if image_id: 
                gcs_images[image_id] = f"gs://{bucket_name}/{blob.name}"
    st.write(f"Found {len(gcs_images)} input images in GCS.")
    return gcs_images

@st.cache_data(ttl=300)
def get_gcs_output_videos(_storage_client, bucket_name, prefixes):
    st.write("Scanning GCS Output folders...")
    gcs_videos = {} # Key: stem, Value: full path
    valid_extensions = ('.mp4', '.webp')
    
    for prefix in prefixes:
        blobs = _storage_client.list_blobs(bucket_name, prefix=prefix)
        for blob in blobs:
            if blob.name.endswith(valid_extensions):
                video_stem = Path(blob.name).stem
                if video_stem:
                    gcs_videos[video_stem] = blob.name 
    st.write(f"Found {len(gcs_videos)} generated videos in GCS.")
    return gcs_videos

@st.cache_data(ttl=300)
def get_bq_all_rows(_bq_client, table_id):
    st.write("Querying BigQuery for all existing rows...")
    try:
        query = f"SELECT image_id, generation_status, video_id FROM `{table_id}` WHERE image_id IS NOT NULL"
        results = _bq_client.query(query).result()
        row_map = {row.image_id: dict(row) for row in results}
        st.write(f"Found {len(row_map)} existing rows in BigQuery.")
        return row_map
    except Exception as e:
        if "not found" in str(e).lower():
             st.warning("Table not found. Please click 'Reset Table' to create it.")
             return {}
        st.error(f"Error querying BigQuery: {e}")
        return {}

# --- NEW: Create Table Helper (for Reset) ---
def recreate_bq_table():
    """Drops and recreates the table with the correct schema."""
    schema = [
        bigquery.SchemaField("image_id", "STRING"),
        bigquery.SchemaField("gtin", "STRING"),
        bigquery.SchemaField("source_gcs_path", "STRING"),
        bigquery.SchemaField("generation_status", "STRING"),
        bigquery.SchemaField("generation_attempts", "INTEGER"),
        bigquery.SchemaField("prompt", "STRING"),
        bigquery.SchemaField("video_id", "STRING"),
        bigquery.SchemaField("decision", "STRING"),
        bigquery.SchemaField("notes", "STRING"),
        bigquery.SchemaField("moderator_id", "STRING"),
        bigquery.SchemaField("log_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("last_updated", "TIMESTAMP"),
        # NEW FIELD: Audit trail for who made the last change
        bigquery.SchemaField("last_updated_email", "STRING"),
    ]
    try:
        bq_client.delete_table(BIGQUERY_TABLE, not_found_ok=True)
        table = bigquery.Table(BIGQUERY_TABLE, schema=schema)
        bq_client.create_table(table)
        st.success(f"Table `{BIGQUERY_TABLE}` successfully recreated!")
        return True
    except Exception as e:
        st.error(f"Failed to recreate table: {e}")
        return False

# --- FIXED SYNC FUNCTION ---
def sync_gcs_to_bigquery():
    """
    Compares GCS input/output folders with BigQuery table.
    Handles Streaming Buffer Lock by determining correct status BEFORE insert.
    """
    storage_client, bq_client = get_gcp_clients()
    
    # 1. Gather Data
    gcs_input_map = get_gcs_input_images(storage_client, BUCKET_NAME, GCS_INPUT_PREFIX)
    gcs_output_map = get_gcs_output_videos(storage_client, BUCKET_NAME, VIDEO_OUTPUT_PREFIXES)
    bq_rows_map = get_bq_all_rows(bq_client, BIGQUERY_TABLE)
    bq_existing_ids = set(bq_rows_map.keys())
    
    # 2. Task 1: Handle NEW images (Forward-fill)
    new_image_ids = set(gcs_input_map.keys()) - bq_existing_ids
    rows_to_insert = []
    
    if new_image_ids:
        st.write(f"Found {len(new_image_ids)} new images. Preparing to insert...")
        
        for image_id in new_image_ids:
            source_path = gcs_input_map[image_id]
            gtin = get_gtin_from_path(image_id)
            
            # --- CRITICAL FIX: Check for video BEFORE insert ---
            initial_status = "PENDING"
            initial_video_id = None
            
            image_stem = Path(image_id).stem
            if image_stem in gcs_output_map:
                # If a video is already in the output folder, set status to APPROVAL_PENDING
                initial_status = "APPROVAL_PENDING"
                initial_video_id = gcs_output_map[image_stem]
            # --------------------------------------------------

            rows_to_insert.append({
                "image_id": image_id,
                "gtin": gtin,
                "source_gcs_path": source_path,
                "generation_status": initial_status, # Set correct status now
                "video_id": initial_video_id,        # Set correct video now
                "generation_attempts": 0,
                "last_updated": datetime.utcnow().isoformat(),
                "prompt": get_default_prompt(source_path),
                "decision": None,
                "notes": None,
                "moderator_id": None,
                "log_timestamp": None,
                "last_updated_email": None
            })
            
        try:
            # Insert new rows
            errors = bq_client.insert_rows_json(BIGQUERY_TABLE, rows_to_insert)
            if not errors:
                st.success(f"Successfully loaded {len(rows_to_insert)} new rows.")
            else:
                st.error(f"Errors inserting rows: {errors}")
        except Exception as e:
            st.error(f"An error occurred during BQ insert: {e}")
            
    # 3. Task 2: Update OLD rows (Back-fill) - Only updates existing rows that are wrong.
    st.write("Checking for existing videos to sync (on old rows)...")
    updates_to_run = []
    stem_to_image_id_map = {Path(img_id).stem: img_id for img_id in bq_existing_ids}
    
    for video_stem, video_path in gcs_output_map.items():
        if video_stem in stem_to_image_id_map:
            image_id = stem_to_image_id_map[video_stem]
            bq_row = bq_rows_map[image_id]
            
            # Update if status is currently PENDING AND video_id is null/missing (i.e., it was inserted before video existed)
            if bq_row.get('generation_status') == 'PENDING' and bq_row.get('video_id') is None:
                updates_to_run.append((image_id, video_path))

    if updates_to_run:
        st.write(f"Found {len(updates_to_run)} old rows to update...")
        for image_id, video_path in updates_to_run:
            try:
                query = f"""
                    UPDATE `{BIGQUERY_TABLE}`
                    SET 
                        generation_status = 'APPROVAL_PENDING',
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
                if "streaming buffer" in str(e):
                    pass 
                else:
                    st.error(f"Failed to back-fill {image_id}: {e}")
                
    return len(rows_to_insert), len(updates_to_run)


# --- WebP Conversion ---
def convert_mp4_to_webp(mp4_temp_file_path, webp_temp_file_path):
    """Converts MP4 to WebP using moviepy."""
    try:
        video_clip = VideoFileClip(mp4_temp_file_path)
        video_clip.write_videofile(
            webp_temp_file_path,
            codec='libwebp',
            preset='default',
            ffmpeg_params=[
                '-vf', 'fps=25',
                '-lossless', '0',
                '-q:v', '80', 
                '-loop', '0'
            ],
            audio=False,
            logger=None
        )
        video_clip.close()
        return True
    except Exception as e:
        st.error(f"Failed to convert video: {e}")
        return False

# --- Get Queue ---
@st.cache_data(ttl=60)
def get_videos_to_review():
    print("Querying BQ for videos to review...")
    try:
        query = f"""
            SELECT image_id, video_id, prompt, notes
            FROM `{BIGQUERY_TABLE}`
            WHERE generation_status = 'APPROVAL_PENDING'
              AND decision IS NULL
            ORDER BY last_updated ASC
        """
        results = bq_client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        st.error(f"Error querying BigQuery: {e}")
        return []

# --- Log Decision ---
def update_decision_in_bq(moderator_id, image_id, decision, new_prompt, new_notes, source_video_path=None):
    print(f"Updating decision for {image_id} by {moderator_id}: {decision}")
    
    new_video_path = None 
    new_generation_status = 'MODERATED' 
    
    try:
        if decision == 'approve' and source_video_path:
            with st.spinner("Converting MP4 to WebP..."):
                source_blob = bucket.blob(source_video_path)
                
                with tempfile.NamedTemporaryFile(suffix=".mp4") as mp4_temp:
                    with tempfile.NamedTemporaryFile(suffix=".webp") as webp_temp:
                        
                        st.write("Downloading MP4...")
                        source_blob.download_to_filename(mp4_temp.name)
                        
                        st.write("Converting to WebP...")
                        success = convert_mp4_to_webp(mp4_temp.name, webp_temp.name)
                        if not success: st.stop()

                        video_filename_stem = Path(source_video_path).stem
                        webp_filename = f"{video_filename_stem}.webp"
                        destination_path = os.path.join(GCS_APPROVED_FOLDER, webp_filename)
                        
                        st.write("Uploading WebP...")
                        new_blob = bucket.blob(destination_path)
                        new_blob.upload_from_filename(webp_temp.name)
                        new_video_path = new_blob.name
                
                st.write("Deleting original MP4...")
                source_blob.delete()
                
                st.toast(f"Converted and moved to: {new_video_path}")

        elif (decision == 'regenerate' or decision == 'remove') and source_video_path:
            st.write(f"Deleting: {source_video_path}")
            blob = bucket.blob(source_video_path)
            blob.delete()
            st.toast(f"Deleted old video: {source_video_path}")
            
            if decision == 'regenerate':
                new_generation_status = 'PENDING'
        
    except Exception as e:
        st.error(f"Error handling GCS file: {e}")
        st.stop() 

    try:
        query = f"""
            UPDATE `{BIGQUERY_TABLE}`
            SET decision = @decision, generation_status = @gen_status, prompt = @prompt, notes = @notes, moderator_id = @moderator, log_timestamp = @timestamp, last_updated = @timestamp, video_id = @new_video_path, last_updated_email = @moderator
            WHERE image_id = @image_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("decision", "STRING", decision),
                bigquery.ScalarQueryParameter("gen_status", "STRING", new_generation_status),
                bigquery.ScalarQueryParameter("prompt", "STRING", new_prompt),
                bigquery.ScalarQueryParameter("notes", "STRING", new_notes),
                bigquery.ScalarQueryParameter("moderator", "STRING", moderator_id),
                bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", datetime.utcnow().isoformat()),
                bigquery.ScalarQueryParameter("new_video_path", "STRING", new_video_path),
                bigquery.ScalarQueryParameter("image_id", "STRING", image_id),
            ]
        )
        
        bq_client.query(query, job_config=job_config).result()
        st.toast(f"✅ Decision '{decision}' logged!")
        st.cache_data.clear()
        st.rerun()
    except Exception as e:
        st.error(f"An error occurred updating BigQuery: {e}")

# --- UI ---
st.title("📹 Video Moderation Tool")

if 'moderator_id' not in st.session_state:
    st.sidebar.header("Login")
    moderator_email = st.sidebar.text_input("Please enter your email:", key="moderator_email_input")
    if st.sidebar.button("Login"):
        if moderator_email and '@' in moderator_email:
            st.session_state.moderator_id = moderator_email
            st.rerun()
        else:
            st.sidebar.error("Invalid email.")
    st.info("Please log in to start.")
else:
    moderator_id = st.session_state.moderator_id
    st.sidebar.success(f"Logged in as: **{moderator_id}**")
    st.sidebar.header("Admin Tools")
    
    if st.sidebar.button("🔄 Sync GCS & BigQuery"):
        with st.spinner("Performing 2-way sync..."):
            st.cache_data.clear() 
            new_items, updated_items = sync_gcs_to_bigquery()
            st.cache_data.clear()
            st.sidebar.success("Sync complete!")
            st.rerun()

    # --- FIX: New Reset Logic ---
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ⚠️ **Danger Zone: Reset Table**")
    
    # 1. Confirmation Checkbox
    confirm_reset = st.sidebar.checkbox("Confirm Table Deletion?", key="confirm_reset_checkbox")

    # 2. Dependent Reset Button
    if confirm_reset:
        if st.sidebar.button("🚨 DELETE AND RECREATE TABLE", type="primary"):
            recreate_bq_table()
            st.cache_data.clear()
            st.rerun()
    else:
        st.sidebar.button("Reset Table", disabled=True)
    # --- END FIX ---


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
        initial_notes = current_video_data.get("notes", "")

        if not video_path_gcs:
            st.error(f"Data error: Missing video_id for {image_id}")
            st.stop()

        try:
            blob = bucket.blob(video_path_gcs)
            signed_url = blob.generate_signed_url(version="v4", expiration=timedelta(minutes=15), service_account_email=SERVICE_ACCOUNT_EMAIL)
        except Exception as e:
            st.error(f"Error getting video: {e}")
            st.stop()

        col1, col2 = st.columns([0.4, 0.6])
        with col1:
            if video_path_gcs.endswith('.webp'):
                st.image(signed_url, use_container_width=True)
            else:
                st.video(signed_url)
        with col2:
            st.markdown(f"**Image ID:** `{image_id}`")
            edited_prompt = st.text_area("Prompt:", value=initial_prompt, height=200)
            edited_notes = st.text_area("Notes:", value=initial_notes, height=100)
            
            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("✅ Approve", use_container_width=True):
                    update_decision_in_bq(moderator_id, image_id, "approve", edited_prompt, edited_notes, source_video_path=video_path_gcs)
            with c2:
                if st.button("♻️ Regenerate", use_container_width=True):
                    update_decision_in_bq(moderator_id, image_id, "regenerate", edited_prompt, edited_notes, source_video_path=video_path_gcs)
            with c3:
                if st.button("🗑️ Remove", use_container_width=True):
                    update_decision_in_bq(moderator_id, image_id, "remove", edited_prompt, edited_notes, source_video_path=video_path_gcs)