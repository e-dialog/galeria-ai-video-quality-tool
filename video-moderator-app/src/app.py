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

def copy_blob_between_buckets(gcs_uri: str, source_bucket: Bucket, destination_bucket: Bucket, new_name: str) -> None:
    """Copies a blob from one bucket to another with a new name."""
    source_blob: Blob = Blob.from_uri(gcs_uri, client=storage_client)
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
    
def approve_video(gtin: str, notes: str | None, moderator: str, image_gcs_uri: str, video_gcs_uri: str, prompt: str | None) -> None:
    """Marks the video as approved in BigQuery and moves the image and video to the approved assets bucket."""
    timestamp: str = datetime.now().isoformat()

    destination_image_name: str = f"{gtin}/{image_gcs_uri.split('/')[-1]}"
    copy_blob_between_buckets(image_gcs_uri, processed_assets_bucket, approved_assets_bucket, destination_image_name)
    
    destination_video_name: str = f"{gtin}/{video_gcs_uri.split('/')[-1]}"
    copy_blob_between_buckets(video_gcs_uri, processed_assets_bucket, approved_assets_bucket, destination_video_name)

    log({
        "gtin": gtin,
        "status": "VIDEO_APPROVED",
        "image_gcs_uri": f"gs://{APPROVED_ASSETS_BUCKET_NAME}/{destination_image_name}",
        "video_gcs_uri": f"gs://{APPROVED_ASSETS_BUCKET_NAME}/{destination_video_name}",
        "prompt": prompt,
        "notes": notes,
        "moderator_id": moderator,
        "timestamp": timestamp,
    })
    
    st.toast(f"Video for GTIN {gtin} approved!")

def reject_video(gtin: str, notes: str | None, moderator: str, image_gcs_uri: str, video_gcs_uri: str) -> None:
    """Marks the video as rejected in BigQuery."""
    timestamp: str = datetime.now().isoformat()
    
    destination_image_name: str = f"{gtin}/{image_gcs_uri.split('/')[-1]}"
    copy_blob_between_buckets(image_gcs_uri, processed_assets_bucket, input_assets_bucket, destination_image_name)

    delete_blob(video_gcs_uri)

    log({
        "gtin": gtin,
        "status": "VIDEO_REJECTED",
        "notes": notes,
        "moderator_id": moderator,
        "timestamp": timestamp,
    })
    
    st.toast(f"Video for GTIN {gtin} rejected!")

def regenerate_video(gtin: str, prompt: str | None, moderator: str, notes: str | None, image_gcs_uri: str, video_gcs_uri: str) -> None:
    """Marks the video for regeneration in BigQuery."""
    timestamp: str = datetime.now().isoformat()
    
    destination_image_name: str = f"{gtin}/{image_gcs_uri.split('/')[-1]}"
    copy_blob_between_buckets(image_gcs_uri, processed_assets_bucket, input_assets_bucket, destination_image_name)
    
    delete_blob(video_gcs_uri)
    
    log({
        "gtin": gtin,
        "status": "VIDEO_REGENERATION_REQUESTED",
        "image_gcs_uri": f"gs://{INPUT_ASSETS_BUCKET_NAME}/{destination_image_name}",
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
        image_path_gcs: str = current_video_data["image_gcs_uri"]
        video_path_gcs: str = current_video_data["video_gcs_uri"]
        initial_prompt: str = current_video_data["prompt"]
        initial_notes: str = current_video_data.get("notes", "")

        try:
            image_file_url: str = f"https://storage.cloud.google.com/{image_path_gcs.replace("gs://", "")}"
            video_file_url: str = f"https://storage.cloud.google.com/{video_path_gcs.replace("gs://", "")}"
           
        except Exception as e:
            st.error(f"Error getting video: {e}")
            st.stop()

        col1, col2 = st.columns([0.5, 0.5])
        with col1:
            st.markdown(f"**GTIN:** `{gtin}`")
            with st.container(border=False, horizontal_alignment="center"):
                st.image(image_file_url, caption="Source Image", width=400)
            
            edited_prompt = st.text_area("Prompt:", value=initial_prompt, height=200)
            edited_notes = st.text_area("Notes:", value=initial_notes, height=100)

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
                            image_gcs_uri=image_path_gcs,
                            video_gcs_uri=video_path_gcs,
                            prompt=edited_prompt
                        )
                        
                        st.session_state.video_queue.pop(0)
                        
                        current_video_data = st.session_state.video_queue[0]
                        gtin = current_video_data["gtin"]
                        image_path_gcs = current_video_data["image_gcs_uri"]
                        video_path_gcs = current_video_data["video_gcs_uri"]
                        initial_prompt = current_video_data["prompt"]
                        initial_notes = current_video_data.get("notes", "")
                        
                        st.rerun()
                        
                with c2:                
                    if st.button("♻️ Regenerate", use_container_width=True, disabled=True, help="Regeneration is currently disabled."):
                        regenerate_video(
                            gtin=gtin,
                            prompt=edited_prompt,
                            image_gcs_uri=image_path_gcs,
                            video_gcs_uri=video_path_gcs,
                            moderator=st.session_state.moderator_id,
                            notes=edited_notes
                        )
                        
                        st.session_state.video_queue.pop(0)
                        
                        current_video_data = st.session_state.video_queue[0]
                        gtin = current_video_data["gtin"]
                        image_path_gcs = current_video_data["image_gcs_uri"]
                        video_path_gcs = current_video_data["video_gcs_uri"]
                        initial_prompt = current_video_data["prompt"]
                        initial_notes = current_video_data.get("notes", "")

                        st.rerun()
                    
                with c3:
                    if st.button("🗑️ Remove", use_container_width=True):
                        reject_video(
                            gtin=gtin,
                            moderator=st.session_state.moderator_id,
                            image_gcs_uri=image_path_gcs,
                            video_gcs_uri=video_path_gcs,
                            notes=edited_notes
                        )
                        
                        st.session_state.video_queue.pop(0)
                        
                        current_video_data = st.session_state.video_queue[0]
                        gtin = current_video_data["gtin"]
                        image_path_gcs = current_video_data["image_gcs_uri"]
                        video_path_gcs = current_video_data["video_gcs_uri"]
                        initial_prompt = current_video_data["prompt"]
                        initial_notes = current_video_data.get("notes", "")

                        st.rerun()