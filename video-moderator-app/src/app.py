from datetime import datetime

import streamlit as st
from cached_resources import get_bigquery_client, get_data, get_storage_client
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.bigquery import Table
from google.cloud.storage import Blob, Bucket
from google.cloud.storage import Client as StorageClient

st.set_page_config(layout="wide", page_title="Video Moderation Tool")

# --- Configuration ---
INPUT_ASSETS_BUCKET_NAME = "galeria-veo3-input-assets-galeria-retail-api-dev"
PROCESSED_ASSETS_BUCKET_NAME = "galeria-veo3-processed-assets-galeria-retail-api-dev"
APPROVED_ASSETS_BUCKET_NAME = "galeria-veo3-approved-assets-galeria-retail-api-dev"
GENERATED_VIDEOS_VIEW = "galeria-retail-api-dev.image_processing_logs.view_videos_ready_for_review"
SERVICE_ACCOUNT_EMAIL = "video-moderator-galeria-retail@galeria-retail-api-dev.iam.gserviceaccount.com"

bigquery_client: BigQueryClient = get_bigquery_client()
storage_client: StorageClient = get_storage_client()

input_assets_bucket: Bucket = storage_client.bucket(INPUT_ASSETS_BUCKET_NAME)
processed_assets_bucket: Bucket = storage_client.bucket(PROCESSED_ASSETS_BUCKET_NAME)
approved_assets_bucket: Bucket = storage_client.bucket(APPROVED_ASSETS_BUCKET_NAME)

def log(row: dict) -> None:
    """Logs the video moderation event to BigQuery."""
    try:
        table: Table = bigquery_client.get_table(BIGQUERY_TABLE)  # type: ignore
        bigquery_client.insert_rows(
            table=table,
            rows=[row]
        )
    except Exception as e:
        st.error(f"Error logging to BigQuery: {e}")

def get_gtin_image_blobs(bucket: Bucket, gtin: str) -> list[Blob]:
    """Helper to get all image blobs for a GTIN in a specific bucket."""
    blobs = list(storage_client.list_blobs(bucket, prefix=f"{gtin}/"))
    # Filter for images (webp, png, jpg, etc.) and sort alphabetically to ensure consistent Front/Back ordering
    images = [b for b in blobs if b.content_type and b.content_type.startswith("image/")]
    images.sort(key=lambda x: x.name) 
    return images

def copy_blob_between_buckets(source_blob: Blob, destination_bucket: Bucket, new_name: str) -> None:
    """Copies a blob from one bucket to another with a new name."""
    source_bucket.copy_blob(
        blob=source_blob,
        destination_bucket=destination_bucket,
        new_name=new_name
    )
    source_blob.delete()

def delete_blob(gcs_uri: str) -> None:
    """Deletes a blob from its bucket."""
    blob: Blob = Blob.from_uri(gcs_uri, client=storage_client)
    blob.delete()
    
def approve_video(gtin: str, notes: str | None, moderator: str, video_gcs_uri: str, prompt: str | None) -> None:
    """Marks the video as approved in BigQuery and moves ALL associated images and video to approved assets."""
    timestamp: str = datetime.now().isoformat()

    # Move ALL images (Front + Back OR Single) found in processed bucket for this GTIN
    image_blobs = get_gtin_image_blobs(processed_assets_bucket, gtin)
    
    # We log the first image found as the primary reference in BQ, but move all
    primary_image_uri = ""
    
    for blob in image_blobs:
        destination_name = f"{gtin}/{blob.name.split('/')[-1]}"
        copy_blob_between_buckets(blob, approved_assets_bucket, destination_name)
        if not primary_image_uri:
             primary_image_uri = f"gs://{APPROVED_ASSETS_BUCKET_NAME}/{destination_name}"

    # Move Video
    destination_video_name: str = f"{gtin}/{video_gcs_uri.split('/')[-1]}"
    video_blob = Blob.from_uri(video_gcs_uri, client=storage_client)
    processed_assets_bucket.copy_blob(video_blob, approved_assets_bucket, destination_video_name)
    video_blob.delete()

    log({
        "gtin": gtin,
        "status": "VIDEO_APPROVED",
        "image_gcs_uri": primary_image_uri,
        "video_gcs_uri": f"gs://{APPROVED_ASSETS_BUCKET_NAME}/{destination_video_name}",
        "prompt": prompt,
        "notes": notes,
        "moderator_id": moderator,
        "timestamp": timestamp,
    })
    
    st.toast(f"Video for GTIN {gtin} approved!")

def reject_video(gtin: str, notes: str | None, moderator: str, video_gcs_uri: str) -> None:
    """Marks the video as rejected and moves images back to input."""
    timestamp: str = datetime.now().isoformat()
    
    # Move ALL images back to input
    image_blobs = get_gtin_image_blobs(processed_assets_bucket, gtin)
    
    for blob in image_blobs:
        destination_name = f"{gtin}/{blob.name.split('/')[-1]}"
        copy_blob_between_buckets(blob, input_assets_bucket, destination_name)

    delete_blob(video_gcs_uri)

    log({
        "gtin": gtin,
        "status": "VIDEO_REJECTED",
        "notes": notes,
        "moderator_id": moderator,
        "timestamp": timestamp,
    })
    
    st.toast(f"Video for GTIN {gtin} rejected!")

def regenerate_video(gtin: str, prompt: str | None, moderator: str, notes: str | None, video_gcs_uri: str) -> None:
    """Marks the video for regeneration and moves images back to input."""
    timestamp: str = datetime.now().isoformat()
    
    # Move ALL images back to input
    image_blobs = get_gtin_image_blobs(processed_assets_bucket, gtin)
    primary_image_uri = ""

    for blob in image_blobs:
        destination_name = f"{gtin}/{blob.name.split('/')[-1]}"
        copy_blob_between_buckets(blob, input_assets_bucket, destination_name)
        if not primary_image_uri:
            primary_image_uri = f"gs://{INPUT_ASSETS_BUCKET_NAME}/{destination_name}"
    
    delete_blob(video_gcs_uri)
    
    log({
        "gtin": gtin,
        "status": "VIDEO_REGENERATION_REQUESTED",
        "image_gcs_uri": primary_image_uri,
        "prompt": prompt,
        "notes": notes,
        "moderator_id": moderator,
        "timestamp": timestamp,
    })

    st.toast(f"Regeneration requested for GTIN {gtin}!")

def get_videos_to_review() -> list[dict]:
    print("Fetching videos that are ready to be reviewed...")

    try:
        return get_data(f"SELECT * FROM `{GENERATED_VIDEOS_VIEW}`")

    except Exception as e:
        print(f"Error querying BigQuery: {e}")
        return []

# --- UI ---
st.title("📹 Video Moderation Tool")

if 'moderator_id' not in st.session_state:
    st.sidebar.header("Login")
    
    moderator_email: str = st.sidebar.text_input(
        label="Please enter your email:", 
        key="moderator_email_input"
    )
    
    if st.sidebar.button("Login"):
        if moderator_email and '@' in moderator_email:
            st.session_state.moderator_id = moderator_email
            st.rerun()
        else:
            st.sidebar.error("Invalid email.")
    
    st.info("Please log in to start.")
    
else:
    moderator_id: str = st.session_state.moderator_id
    st.sidebar.success(f"Logged in as: **{moderator_id}**")

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
        gtin: str = current_video_data["gtin"]
        video_path_gcs: str = current_video_data["video_gcs_uri"]
        initial_prompt: str = current_video_data["prompt"]
        initial_notes: str = current_video_data.get("notes", "")

        try:
            video_file_url: str = f"https://storage.cloud.google.com/{video_path_gcs.replace('gs://', '')}"
            
            # Fetch all associated images (Front + Back) for this GTIN
            image_blobs = get_gtin_image_blobs(processed_assets_bucket, gtin)
            image_urls = [f"https://storage.cloud.google.com/{PROCESSED_ASSETS_BUCKET_NAME}/{b.name}" for b in image_blobs]
           
        except Exception as e:
            st.error(f"Error getting assets: {e}")
            st.stop()

        # UI LAYOUT: 50/50 Split
        col1, col2 = st.columns([0.5, 0.5])
        
        # --- LEFT COLUMN: INPUTS (Images + Prompt) ---
        with col1:
            st.markdown(f"**GTIN:** `{gtin}`")
            
            # Compact Image Display Logic
            if image_urls:
                if len(image_urls) == 1:
                    # LEGACY: Single Image -> Display centered with max width
                    # width=300 ensures it doesn't take up the whole screen height
                    st.image(image_urls[0], caption="Source Image", width=300)
                    
                else:
                    # NEW: 2+ Images -> Display side-by-side in sub-columns
                    img_c1, img_c2 = st.columns(2)
                    with img_c1:
                        st.image(image_urls[0], caption="Front", use_container_width=True)
                    with img_c2:
                        # Safety check if index 1 exists (implied by len > 1)
                        if len(image_urls) > 1:
                            st.image(image_urls[1], caption="Back", use_container_width=True)
            else:
                st.warning("No reference images found in processed bucket.")
            
            # Reduced height for prompt to save screen space (was 200)
            edited_prompt = st.text_area("Prompt:", value=initial_prompt, height=120)
            edited_notes = st.text_area("Notes:", value=initial_notes, height=80)

        # --- RIGHT COLUMN: OUTPUT (Video + Actions) ---
        with col2:

             with st.container(border=False, horizontal_alignment="center"):
                st.video(video_file_url, width=400, autoplay=True)

                c1, c2, c3 = st.columns(3)
                with c1:
                    if st.button("✅ Approve", use_container_width=True):
                        approve_video(
                            gtin=gtin,
                            notes=edited_notes,
                            moderator=st.session_state.moderator_id,
                            video_gcs_uri=video_path_gcs,
                            prompt=edited_prompt
                        )
                        st.session_state.video_queue.pop(0)
                        st.rerun()
                        
                with c2:                
                    if st.button("♻️ Regenerate", use_container_width=True, disabled=True, help="Regeneration is currently disabled."):
                        regenerate_video(
                            gtin=gtin,
                            prompt=edited_prompt,
                            video_gcs_uri=video_path_gcs,
                            moderator=st.session_state.moderator_id,
                            notes=edited_notes
                        )
                        st.session_state.video_queue.pop(0)
                        st.rerun()
                    
                with c3:
                    if st.button("🗑️ Remove", use_container_width=True):
                        reject_video(
                            gtin=gtin,
                            moderator=st.session_state.moderator_id,
                            video_gcs_uri=video_path_gcs,
                            notes=edited_notes
                        )
                        st.session_state.video_queue.pop(0)
                        st.rerun()