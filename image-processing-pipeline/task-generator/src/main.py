"""
 Cloud Function triggered by a GCS file upload into the new assets bucket.
    1. Logs the event to BigQuery
    2. Creates a task in the rate-limited Cloud Tasks queue.
    3. Adds metadata to the image in Cloud Storage.
"""

import json
import os
import base64
import io
from datetime import datetime
from PIL import Image

from google.cloud.bigquery import Table, Client as BigQueryClient
from google.cloud.tasks_v2 import CloudTasksClient, HttpRequest, Task
from google.cloud.storage import Client as StorageClient, Blob, Bucket

# Environment variables
PROJECT_ID: str | None = os.environ.get('PROJECT_ID')
TASK_QUEUE_NAME: str | None = os.environ.get('TASK_QUEUE_NAME')
TASK_QUEUE_LOCATION: str | None = os.environ.get('TASK_QUEUE_LOCATION')
BIGQUERY_VIDEO_LOGS_TABLE_ID: str | None = os.environ.get('BIGQUERY_VIDEO_LOGS_TABLE_ID')

assert PROJECT_ID is not None, "PROJECT_ID environment variable is required"
assert TASK_QUEUE_NAME is not None, "TASK_QUEUE_NAME environment variable is required"
assert TASK_QUEUE_LOCATION is not None, "TASK_QUEUE_LOCATION environment variable is required"
assert BIGQUERY_VIDEO_LOGS_TABLE_ID is not None, "BIGQUERY_VIDEO_LOGS_TABLE_ID environment variable is required"

# Google Client libraries
bigquery_client: BigQueryClient = BigQueryClient()
cloud_tasks_client: CloudTasksClient = CloudTasksClient()
storage_client: StorageClient = StorageClient()


def unpack_event_message(event) -> dict:
    """Unpacks the Pub/Sub message and returns the email content as a dictionary"""
    data: str = base64.b64decode(event['data']).decode('utf-8')
    return json.loads(data)


def calculate_aspect_ratio(image_gcs_uri: str) -> str:
    """Calculates the aspect ratio of the image stored in GCS."""
    source_image_blob: Blob = Blob.from_uri(image_gcs_uri, client=storage_client)
    image_bytes: bytes = source_image_blob.download_as_bytes()

    with Image.open(io.BytesIO(image_bytes)) as image:
        width, height = image.size
        print(f"Image dimensions: width={width}, height={height}")
        return "16:9" if width >= height else "9:16"


def log(gtin: str, category: str, front_image_gcs_uri: str) -> None:
    """Logs the video generation event to BigQuery."""
    ingestion_time: str = datetime.now().isoformat()

    try:
        table: Table = bigquery_client.get_table(BIGQUERY_VIDEO_LOGS_TABLE_ID)  # type: ignore
        bigquery_client.insert_rows(
            table=table,
            rows=[
                {
                    "gtin": gtin,
                    "category": category,
                    "status": "QUEUED_FOR_VIDEO_GENERATION",
                    "image_gcs_uri": front_image_gcs_uri, 
                    "timestamp": ingestion_time,
                }
            ]
        )

    except Exception as e:
        print(f"Error logging to BigQuery: {e}")
        # Log the error, but do not raise to avoid interrupting the main flow


def enqueue_task(gtin: str, category: str, front_gcs_uri: str, back_gcs_uri: str) -> None:
    """Enqueues a task in Cloud Tasks for processing the image."""
    parent: str = cloud_tasks_client.queue_path(
        project=PROJECT_ID,  # type: ignore
        location=TASK_QUEUE_LOCATION,  # type: ignore
        queue=TASK_QUEUE_NAME  # type: ignore
    )

    # Use front image for aspect ratio calculation
    aspect_ratio: str = calculate_aspect_ratio(front_gcs_uri)

    response: Task = cloud_tasks_client.create_task(
        parent=parent,
        task=Task(
            http_request=HttpRequest(
                http_method="POST",
                # We override this in the Terraform config with the actual endpoint
                url="https://placeholder-host/",
                headers={"Content-Type": "application/json"},
                body=json.dumps(
                    {
                        "gtin": gtin,
                        "category": category,
                        "front_image_gcs_uri": front_gcs_uri,
                        "back_image_gcs_uri": back_gcs_uri,
                        "mime_type": f"image/{front_gcs_uri.split('.')[-1].lower()}",
                        "aspect_ratio": aspect_ratio
                    }
                ).encode()
            )
        )
    )

    print(f"Task successfully created: {response.name}")


def main(event: dict, context: dict) -> tuple[str, int]:
    data: dict = unpack_event_message(event)
    print(data)

    file_name: str | None = data.get('name')
    bucket_name: str | None = data.get('bucket')

    assert file_name is not None, "File name is required in the event"
    assert bucket_name is not None, "Bucket name is required in the event"

    # Expecting format: female_clothes/GTIN_front.webp
    path_parts = file_name.split('/')
    
    # Validation to ensure file is in a folder
    if len(path_parts) < 2:
        print(f"File {file_name} is not in a subfolder. Skipping.")
        return "SKIPPED_NO_FOLDER", 200

    category: str = path_parts[0] # e.g., 'female_clothes'
    gtin: str = path_parts[-1].split('_')[0]
    
    # List all files for this specific Category + GTIN to check if we have the pair
    prefix = f"{category}/{gtin}"
    blobs = list(storage_client.list_blobs(bucket_name, prefix=prefix))
    
    # Filter for image files only
    image_blobs = [b for b in blobs if b.name.endswith(('.webp', '.png', '.jpg', '.jpeg'))]
    
    if len(image_blobs) < 2:
        print(f"Waiting for pair. Found {len(image_blobs)} images for GTIN {gtin} in {category} so far.")
        return "WAITING_FOR_PAIR", 200

    # Sort images to ensure consistent assignment (Front/Back)
    image_blobs.sort(key=lambda x: x.name)
    
    # Assumption: Alphabetical sort puts Front before Back (or _01 before _02)
    front_image_blob = image_blobs[0]
    back_image_blob = image_blobs[1]
    
    front_gcs_uri: str = f"gs://{bucket_name}/{front_image_blob.name}"
    back_gcs_uri: str = f"gs://{bucket_name}/{back_image_blob.name}"

    print(f"Processing new asset pair for category '{category}': {front_gcs_uri} + {back_gcs_uri}")

    # Enqueue task for video generation with category info
    enqueue_task(gtin, category, front_gcs_uri, back_gcs_uri)

    # Log event
    log(gtin, category, front_gcs_uri)

    return "OK", 200


if __name__ == "__main__":
    image_gcs_uri: str = "gs://galeria-veo3-input-assets-galeria-retail-api-dev/female_clothes/2246065552629_09.webp"
    # aspect_ratio: str = calculate_aspect_ratio(image_gcs_uri)
    # print(f"Aspect ratio for {image_gcs_uri} is {aspect_ratio}")