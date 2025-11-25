"""
 Cloud Function triggered by a GCS file upload into the new assets bucket.
    1. Logs the event to BigQuery
    2. Creates a task in the rate-limited Cloud Tasks queue.
"""

import json
import os
import base64
from datetime import datetime

from google.cloud.bigquery import Table, Client as BigQueryClient
from google.cloud.tasks_v2 import CloudTasksClient, HttpRequest, Task

# Environment variables
PROJECT_ID: str | None = os.environ.get('PROJECT_ID')
TASK_QUEUE_NAME: str | None = os.environ.get('TASK_QUEUE_NAME')
TASK_QUEUE_LOCATION: str | None = os.environ.get('TASK_QUEUE_LOCATION')
BIGQUERY_VIDEO_LOGS_TABLE_ID: str | None = os.environ.get(
    'BIGQUERY_VIDEO_LOGS_TABLE_ID')

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

def log(gtin: str, category: str, assets: list[str]) -> None:
    """Logs the video generation event to BigQuery."""
    ingestion_time: str = datetime.now().isoformat()

    try:
        table: Table = bigquery_client.get_table(
            BIGQUERY_VIDEO_LOGS_TABLE_ID)  # type: ignore
        bigquery_client.insert_rows(
            table=table,
            rows=[
                {
                    "gtin": gtin,
                    "category": category,
                    "status": "QUEUED_FOR_VIDEO_GENERATION",
                    "reference_image_gcs_uris": assets,
                    "timestamp": ingestion_time,
                }
            ]
        )

    except Exception as e:
        print(f"Error logging to BigQuery: {e}")
        # Log the error, but do not raise to avoid interrupting the main flow


def enqueue_task(gtin: str, category: str, assets: list[str]) -> None:
    """Enqueues a task in Cloud Tasks for processing the image."""
    parent: str = cloud_tasks_client.queue_path(
        project=PROJECT_ID,  # type: ignore
        location=TASK_QUEUE_LOCATION,  # type: ignore
        queue=TASK_QUEUE_NAME  # type: ignore
    )

    # Use front image for aspect ratio calculation
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
                        "assets": assets
                    }
                ).encode()
            )
        )
    )

    print(f"Task successfully created: {response.name}")


def main(event: dict, context: dict) -> tuple[str, int]:
    """
    Example input data
    {
        "gtin": "100",
        "category": "underwear",
        "assets": [
            "gs://galeria-veo3-input-assets-galeria-retail-api-dev/underwear/100_01.webp",
            "gs://galeria-veo3-input-assets-galeria-retail-api-dev/underwear/100_02.webp",
            "gs://galeria-veo3-input-assets-galeria-retail-api-dev/underwear/100_03.webp"
        ]
    }
    """
    data: dict = unpack_event_message(event)
    
    gtin: str | None = data.get("gtin")
    category: str | None = data.get("category")
    assets: list[str] = data.get("assets", [])
    
    assert gtin is not None, "GTIN is required in the event data"
    assert category is not None, "Category is required in the event data"
    
    # Enqueue task for video generation with category info
    enqueue_task(gtin, category, assets)

    # Log event
    log(gtin, category, assets)

    return "OK", 200


if __name__ == "__main__":
    image_gcs_uri: str = "gs://galeria-veo3-input-assets-galeria-retail-api-dev/female_clothes/2246065552629_09.webp"
    # aspect_ratio: str = calculate_aspect_ratio(image_gcs_uri)
    # print(f"Aspect ratio for {image_gcs_uri} is {aspect_ratio}")
