import streamlit as st
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.bigquery import QueryJob
from google.cloud.storage import Client as StorageClient

GENERATED_VIDEOS_VIEW: str = "galeria-retail-api-dev.image_processing_logs.view_videos_ready_for_review"


@st.cache_resource
def get_bigquery_client() -> BigQueryClient:
    return BigQueryClient()


@st.cache_resource
def get_storage_client() -> StorageClient:
    return StorageClient()


@st.cache_data(ttl=3600)  # Cache data for 1 hour
def get_data(query: str) -> list[dict]:
    bigquery_client: BigQueryClient = get_bigquery_client()
    query_job: QueryJob = bigquery_client.query(query)

    result = query_job.result()
    return [dict(row) for row in result]


def get_videos_to_review() -> list[dict]:
    print("Fetching videos that are ready to be reviewed...")

    try:
        return get_data(f"SELECT * FROM `{GENERATED_VIDEOS_VIEW}`")

    except Exception as e:
        print(f"Error querying BigQuery: {e}")
        return []
