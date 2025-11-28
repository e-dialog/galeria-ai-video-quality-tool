import os
from datetime import datetime

from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.bigquery import Table

BIGQUERY_VIDEO_LOGS_TABLE_ID: str | None = os.environ.get("BIGQUERY_VIDEO_LOGS_TABLE_ID")
assert BIGQUERY_VIDEO_LOGS_TABLE_ID is not None, "BIGQUERY_VIDEO_LOGS_TABLE_ID environment variable is required"

bigquery_client: BigQueryClient = BigQueryClient()


def log(row: dict) -> None:
    """Logs the video generation event to BigQuery."""
    try:
        table: Table = bigquery_client.get_table(BIGQUERY_VIDEO_LOGS_TABLE_ID)  # type: ignore
        bigquery_client.insert_rows(
            table=table,
            rows=[row]
        )

    except Exception as e:
        print(f"Error logging to BigQuery: {e}")
        # Log the error, but do not raise to avoid interrupting the main flow


def log_error(gtin: str, assets: str, notes: str) -> None:
    """Logs the video generation error to BigQuery."""
    ingestion_time: str = datetime.now().isoformat()
    log({
        "gtin": gtin,
        "status": "VIDEO_GENERATION_FAILED",
        "reference_image_gcs_uris": assets,
        "notes": notes,
        "timestamp": ingestion_time,
    })


def log_success(
    gtin: str,
    reference_image_gcs_uris: list[str],
    video_gcs_uri: str,
    prompt_used: str,
    category: str
) -> None:
    """Logs the video generation event to BigQuery."""
    ingestion_time: str = datetime.now().isoformat()

    log({
        "gtin": gtin,
        "status": "VIDEO_GENERATION_COMPLETED",
        "reference_image_gcs_uris": reference_image_gcs_uris,
        "category": category,
        "video_gcs_uri": video_gcs_uri,
        "prompt": prompt_used,
        "timestamp": ingestion_time,
    })
