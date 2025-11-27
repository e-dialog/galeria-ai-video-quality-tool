from datetime import datetime
import streamlit as st

from cached_resources import get_storage_client
from google.cloud.storage import Blob, Bucket
from google.cloud.storage import Client as StorageClient
from utils.logging_utilities import log

INPUT_ASSETS_BUCKET_NAME: str = "galeria-veo3-input-assets-galeria-retail-api-dev"
PROCESSED_ASSETS_BUCKET_NAME: str = "galeria-veo3-processed-assets-galeria-retail-api-dev"
APPROVED_ASSETS_BUCKET_NAME: str = "galeria-veo3-approved-assets-galeria-retail-api-dev"

storage_client: StorageClient = get_storage_client()

input_assets_bucket: Bucket = storage_client.bucket(INPUT_ASSETS_BUCKET_NAME)
processed_assets_bucket: Bucket = storage_client.bucket(PROCESSED_ASSETS_BUCKET_NAME)
approved_assets_bucket: Bucket = storage_client.bucket(APPROVED_ASSETS_BUCKET_NAME)

def copy_blob_between_buckets(source_blob: Blob, source_bucket: Bucket, destination_bucket: Bucket, new_name: str) -> None:
    """Copies a blob from one bucket to another with a new name. Deletes the old blob afterwards."""
    source_bucket.copy_blob(
        blob=source_blob,
        destination_bucket=destination_bucket,
        new_name=new_name
    )
    source_blob.delete()

def delete_blob(gcs_uri: str) -> None:
    """Deletes a blob given its GCS URI."""
    blob: Blob = Blob.from_uri(gcs_uri, client=storage_client)
    blob.delete()

def initiate_blob_copy(gtin: str, source_uris: list[str], source_bucket: Bucket, destination_bucket: Bucket) -> None:
    for gcs_uri in source_uris:
        destination_file_name: str = f"{gtin}/{gcs_uri.split('/')[-1]}"
        blob: Blob = Blob.from_uri(gcs_uri, client=storage_client)
        copy_blob_between_buckets(blob, source_bucket, destination_bucket, destination_file_name)

def fix_reference_image_uris(gtin: str, reference_image_gcs_uris: list[str]) -> list[str]:
    """Fixes reference image URIs to point to processed assets if they still point to input assets."""
    for index, gcs_uri in enumerate(reference_image_gcs_uris):
        if "input-assets" in gcs_uri.split("/")[0]:
            # Change gs://galeria-veo3-input-assets-galeria-retail-api-dev/male_clothes/4062742342943_01.jpg
            # to gs://galeria-veo3-processed-assets-galeria-retail-api-dev/4062742342943/4062742342943_01.jpg
            new_uri = f"gs://{PROCESSED_ASSETS_BUCKET_NAME}/{gtin}/{gcs_uri.split('/')[-1]}"
            reference_image_gcs_uris[index] = new_uri
            
    return reference_image_gcs_uris

def approve_video(
    gtin: str, 
    notes: str | None, 
    moderator: str, 
    prompt: str | None, 
    category: str, 
    video_gcs_uri: str, 
    reference_image_gcs_uris: list[str]
) -> None:
    """Marks the video as approved in BigQuery and moves ALL associated images and video to approved assets."""
    timestamp: str = datetime.now().isoformat()

    # Fix for logs containing the wrong URI
    reference_image_gcs_uris = fix_reference_image_uris(gtin, reference_image_gcs_uris)

    # Move Reference Images
    source_uris: list[str] = reference_image_gcs_uris + [video_gcs_uri]
    initiate_blob_copy(gtin, source_uris, processed_assets_bucket, approved_assets_bucket)
    
    video_destination_name: str = f"{gtin}/{video_gcs_uri.split('/')[-1]}"
    reference_image_destination_names: list[str] = [f"gs://{APPROVED_ASSETS_BUCKET_NAME}/{gtin}/{uri.split('/')[-1]}" for uri in reference_image_gcs_uris]
    
    log({
        "gtin": gtin,
        "status": "VIDEO_APPROVED",
        "reference_image_gcs_uris": reference_image_destination_names,
        "video_gcs_uri": f"gs://{APPROVED_ASSETS_BUCKET_NAME}/{video_destination_name}",
        "category": category,
        "prompt": prompt,
        "notes": notes,
        "moderator_id": moderator,
        "timestamp": timestamp,
    })

    st.toast(f"Video for GTIN {gtin} approved!")


def reject_video(
    gtin: str, 
    notes: str | None, 
    moderator: str, 
    video_gcs_uri: str,
    category: str,
    reference_image_gcs_uris: list[str]
) -> None:
    """Marks the video as rejected and moves images back to input."""
    timestamp: str = datetime.now().isoformat()

    # Fix for logs containing the wrong URI
    reference_image_gcs_uris = fix_reference_image_uris(gtin, reference_image_gcs_uris)

    for gcs_uri in reference_image_gcs_uris:
        blob: Blob = Blob.from_uri(gcs_uri, client=storage_client)
        destination_name: str = f"{category}/{gcs_uri.split('/')[-1]}"
        copy_blob_between_buckets(blob, processed_assets_bucket, input_assets_bucket, destination_name)

    delete_blob(video_gcs_uri)

    log({
        "gtin": gtin,
        "status": "VIDEO_REJECTED",
        "notes": notes,
        "category": category,
        "reference_image_gcs_uris": reference_image_gcs_uris,
        "moderator_id": moderator,
        "timestamp": timestamp,
    })

    st.toast(f"Video for GTIN {gtin} rejected!")


def regenerate_video(
    gtin: str, 
    prompt: str | None, 
    moderator: str, 
    notes: str | None, 
    video_gcs_uri: str
) -> None:
    """Marks the video for regeneration and moves images back to input."""
    pass
