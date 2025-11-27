import json

from typing import Generator

from cached_resources import get_storage_client
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.storage import Bucket, Client as StorageClient

TASK_QUEUE_TOPIC_ID: str = "projects/galeria-retail-api-dev/topics/new-asset-notifications"

publisher: PublisherClient = PublisherClient()
storage_client: StorageClient = get_storage_client()

def publish_task() -> None:
    """Publishes a message to a Pub/Sub topic."""
    for asset in get_new_input_assets():
        print(f"Publishing asset task: {asset['gtin']}")
        publisher.publish(TASK_QUEUE_TOPIC_ID, json.dumps(asset).encode("utf-8"))
        
    print("All new asset tasks published.")


def get_new_input_assets() -> Generator[dict, None, None]:
    """Fetches new input assets from the input GCS bucket that need to be processed into tasks.

    Yields a dictionary for each GTIN group as soon as it is fully collected.
    Assumes blobs are sorted such that all files for a specific GTIN appear contiguously.
    
    new_assets = {
        "gtin": "100",
        "category": "underwear",
        "assets": [
            "gs://galeria-veo3-input-assets-galeria-retail-api-dev/underwear/100_01.webp",
            "gs://galeria-veo3-input-assets-galeria-retail-api-dev/underwear/100_02.webp",
            "gs://galeria-veo3-input-assets-galeria-retail-api-dev/underwear/100_03.webp"
        ]
    }
    
    """
    bucket: Bucket = storage_client.bucket("galeria-veo3-input-assets-galeria-retail-api-dev")
    
    current_gtin: str | None = None
    current_group: dict = {}
    
    for blob in bucket.list_blobs():
        
        # Skip "directory" blobs
        if blob.name.endswith('/'):
            continue

        # Skip non-image files
        if not blob.name.endswith(('.webp', '.png', '.jpg', '.jpeg')):
            continue

        parts: list[str] = blob.name.split('/')
        
        category: str = parts[0]    
        filename: str = parts[-1]    
        gtin: str = filename.split('_')[0]
        uri: str = f"gs://{bucket.name}/{blob.name}"
        
        # If we hit a new GTIN, yield the PREVIOUS group (if it exists)
        if gtin != current_gtin:
            if current_gtin is not None:
                yield current_group
            
            # Reset state for the new GTIN
            current_gtin = gtin
            current_group = {
                "gtin": gtin,
                "category": category,
                "assets": []
            }

        # Add the current asset to the buffer
        current_group["assets"].append(uri)

    # After the loop finishes, don't forget to yield the very last group!
    if current_gtin is not None:
        yield current_group