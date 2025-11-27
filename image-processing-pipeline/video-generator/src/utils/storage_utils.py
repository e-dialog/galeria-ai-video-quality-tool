import os
from datetime import datetime

from google.cloud.storage import Blob, Bucket
from google.cloud.storage import Client as StorageClient
from utils.logging_utils import log_success

INPUT_GCS_BUCKET: str | None = os.getenv("INPUT_GCS_BUCKET")
PROCESSED_GCS_BUCKET: str | None = os.getenv("PROCESSED_GCS_BUCKET")

assert INPUT_GCS_BUCKET is not None, "INPUT_GCS_BUCKET environment variable is required"
assert PROCESSED_GCS_BUCKET is not None, "PROCESSED_GCS_BUCKET environment variable is required"

# Google Client libraries
storage_client: StorageClient = StorageClient()

input_asset_bucket: Bucket = storage_client.bucket(INPUT_GCS_BUCKET)
processed_video_bucket: Bucket = storage_client.bucket(PROCESSED_GCS_BUCKET)


def copy_blob_between_buckets(source_uri: str, source_bucket: Bucket, destination_bucket: Bucket, new_name: str) -> None:
    """Copies a blob from one bucket to another with a new name. Deletes the old blob afterwards."""
    source_blob = Blob.from_uri(source_uri, client=source_bucket.client)

    source_bucket.copy_blob(
        blob=source_blob,
        destination_bucket=destination_bucket,
        new_name=new_name
    )

    source_blob.delete()


def move_assets_to_processed(gtin: str, reference_image_gcs_uris: list[str], video_gcs_uri: str, prompt: str) -> None:
    """Organizes the storage files by moving the source images and generated video to their respective folders."""
    for gcs_uri in reference_image_gcs_uris:
        source_file_name: str = f"{gtin}/{gcs_uri.split('/')[-1]}"
        copy_blob_between_buckets(gcs_uri, input_asset_bucket, processed_video_bucket, source_file_name)

        print(f"Reference image moved to processed at: gs://{PROCESSED_GCS_BUCKET}/{source_file_name}")

    # Move the generated video into the gtin folder as well
    generated_video_name: str = f"{gtin}/{video_gcs_uri.split('/')[-1].split('.')[0]}_{datetime.now().isoformat()}.mp4"
    copy_blob_between_buckets(video_gcs_uri, processed_video_bucket, processed_video_bucket, generated_video_name)

    print(f"Video moved after generating and stored at: gs://{PROCESSED_GCS_BUCKET}/{generated_video_name}")

    video_gcs_uri = f"gs://{PROCESSED_GCS_BUCKET}/{generated_video_name}"
    reference_image_destination_names: list[str] = [f"gs://{PROCESSED_GCS_BUCKET}/{gtin}/{uri.split('/')[-1]}" for uri in reference_image_gcs_uris]
    log_success(gtin, reference_image_destination_names, video_gcs_uri, prompt)
