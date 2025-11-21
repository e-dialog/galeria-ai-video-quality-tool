"""
 Cloud Function triggered by a GCS file upload into the new assets bucket.
    1. Logs the event to BigQuery
    2. Creates a task in the rate-limited Cloud Tasks queue.
    3. Adds metadata to the image in Cloud Storage.
"""

import json
import os
import base64
from datetime import datetime

from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.tasks_v2 import CloudTasksClient, HttpRequest, Task

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

def unpack_event_message(event) -> dict:
    """Unpacks the Pub/Sub message and returns the email content as a dictionary"""
    data: str = base64.b64decode(event['data']).decode('utf-8')
    return json.loads(data)

def log(gtin: str, image_gcs_uri: str) -> None:
    """Logs the video generation event to BigQuery."""
    ingestion_time: str = datetime.now().isoformat()

    try:
        bigquery_client.insert_rows(
            table=BIGQUERY_VIDEO_LOGS_TABLE_ID,  # type: ignore
            rows=[
                {
                    "gtin": gtin,
                    "status": "QUEUED_FOR_VIDEO_GENERATION",
                    "image_gcs_uri": image_gcs_uri,
                    "timestamp": ingestion_time,
                }
            ]
        )

    except Exception as e:
        print(f"Error logging to BigQuery: {e}")
        # Log the error, but do not raise to avoid interrupting the main flow


def enqueue_task(gtin: str, gcs_uri: str) -> None:
    """Enqueues a task in Cloud Tasks for processing the image."""
    parent: str = cloud_tasks_client.queue_path(
        project=PROJECT_ID,  # type: ignore
        location=TASK_QUEUE_LOCATION,  # type: ignore
        queue=TASK_QUEUE_NAME  # type: ignore
    )

    response: Task = cloud_tasks_client.create_task(
        parent=parent,
        task=Task(
            http_request=HttpRequest(
                http_method="POST",
                url="https://placeholder-host/", # We override this in the Terraform config with the actual endpoint
                headers={"Content-Type": "application/json"},
                body=json.dumps(
                    {
                        "gtin": gtin,
                        "image_gcs_uri": gcs_uri
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

    gcs_uri: str = f"gs://{bucket_name}/{file_name}"

    # Extract GTIN from file name, expecting filename format: models/239838409823_01.webp
    gtin: str = file_name.split('/')[-1].split('_')[0]

    print(f"Processing new asset: {gcs_uri}")

    # Enqueue task for video generation
    enqueue_task(gtin, gcs_uri)

    # Log event after it got enqueued
    log(gtin, gcs_uri)

    return "OK", 200
