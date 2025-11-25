from cached_resources import get_bigquery_client
from google.cloud.bigquery import Table, Client as BigQueryClient
import streamlit as st

BIGQUERY_LOGGING_TABLE_ID: str = "galeria-retail-api-dev.image_processing_logs.galeria_veo3_video_ingestion_events"

bigquery_client: BigQueryClient = get_bigquery_client()

def log(row: dict) -> None:
    """Logs the video moderation event to BigQuery."""
    try:
        table: Table = bigquery_client.get_table(BIGQUERY_LOGGING_TABLE_ID)  # type: ignore
        bigquery_client.insert_rows(
            table=table,
            rows=[row]
        )
    except Exception as e:
        st.error(f"Error logging to BigQuery: {e}")