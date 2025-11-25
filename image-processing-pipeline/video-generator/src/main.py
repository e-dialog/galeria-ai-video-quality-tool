"""
Video generation module using Google GenAI VEO model.
1. Generates a video from a single image using GenAI VEO model.
2. Uploads the generated video to a specified GCS bucket.
3. Logs the video generation event to BigQuery.
"""

import os
import time
from datetime import datetime

from google.cloud.bigquery import Table, Client as BigQueryClient
from google.cloud.storage import Blob, Bucket
from google.cloud.storage import Client as StorageClient
from google.genai import Client as GenAIClient
from google.genai.types import (GeneratedVideo, GenerateVideosConfig, HttpOptions,
                                GenerateVideosOperation,
                                GenerateVideosResponse, GenerateVideosSource,
                                Image)

# Static constants
VEO_MODEL: str = "veo-3.1-generate-preview"

# Prompts mapped to bucket folder names
PROMPT_MAPPING: dict[str, str] = {
    "female_clothes": "Generate a fashion studio shot of the attached female clothing. The woman in the video is wearing this clothing and is visible from the front before making a casual 180 degree turn to show the clothing from the back.",
    "female_underwear": "Generate a fashion studio shot of the attached female underwear. The woman in the video is wearing this underwear and is visible from the front before making a casual 180 degree turn to show the underwear from the back.",
    "male_clothes": "Generate a fashion studio shot of the attached male clothing. The man in the video is wearing this clothing and is visible from the front before making a casual 180 degree turn to show the clothing from the back.",
    "male_underwear": "Generate a fashion studio shot of the attached male underwear. The man in the video is wearing this underwear and is visible from the front before making a casual 180 degree turn to show the underwear from the back."
}

# Fallback if category isn't matched
DEFAULT_FALLBACK_PROMPT: str = "Generate a fashion studio shot of the attached product. The person in the video is wearing this product and is visible from the front before making a casual 180 degree turn to show the product from the back."

# Environment variables
PROJECT_NUMBER: str | None = os.getenv("PROJECT_NUMBER")
PROJECT_ID: str | None = os.getenv("PROJECT_ID")
INPUT_GCS_BUCKET: str | None = os.getenv("INPUT_GCS_BUCKET")
OUTPUT_GCS_BUCKET: str | None = os.getenv("OUTPUT_GCS_BUCKET")
BIGQUERY_VIDEO_LOGS_TABLE_ID: str | None = os.environ.get(
    "BIGQUERY_VIDEO_LOGS_TABLE_ID")

assert PROJECT_NUMBER is not None, "PROJECT_NUMBER environment variable is required"
assert PROJECT_ID is not None, "PROJECT_ID environment variable is required"
assert INPUT_GCS_BUCKET is not None, "INPUT_GCS_BUCKET environment variable is required"
assert OUTPUT_GCS_BUCKET is not None, "OUTPUT_GCS_BUCKET environment variable is required"
assert BIGQUERY_VIDEO_LOGS_TABLE_ID is not None, "BIGQUERY_VIDEO_LOGS_TABLE_ID environment variable is required"

# Google Client libraries
bigquery_client: BigQueryClient = BigQueryClient()

genai_client: GenAIClient = GenAIClient(
    vertexai=True,
    project=PROJECT_ID,
    location='us-central1',
    http_options=HttpOptions(
        api_version="v1", # type: ignore
        headers={
            # Important! This ensures we do not overshoot on the provisioned throughput capacity
            # https://docs.cloud.google.com/vertex-ai/generative-ai/docs/provisioned-throughput/use-provisioned-throughput#only-provisioned-throughput
            "X-Vertex-AI-LLM-Request-Type": "dedicated"
        }
    )
)

storage_client: StorageClient = StorageClient()


def log(row: dict) -> None:
    """Logs the video generation event to BigQuery."""
    try:
        table: Table = bigquery_client.get_table(
            BIGQUERY_VIDEO_LOGS_TABLE_ID)  # type: ignore
        bigquery_client.insert_rows(
            table=table,
            rows=[row]
        )

    except Exception as e:
        print(f"Error logging to BigQuery: {e}")
        # Log the error, but do not raise to avoid interrupting the main flow


def log_error(gtin: str, image_gcs_uri: str, notes: str) -> None:
    """Logs the video generation error to BigQuery."""
    ingestion_time: str = datetime.now().isoformat()
    log({
        "gtin": gtin,
        "status": "VIDEO_GENERATION_FAILED",
        "image_gcs_uri": image_gcs_uri,
        "notes": notes,
        "timestamp": ingestion_time,
    })


def log_success(gtin: str, image_gcs_uri: str, video_gcs_uri: str, prompt_used: str) -> None:
    """Logs the video generation event to BigQuery."""
    ingestion_time: str = datetime.now().isoformat()

    log({
        "gtin": gtin,
        "status": "VIDEO_GENERATION_COMPLETED",
        "image_gcs_uri": image_gcs_uri,
        "video_gcs_uri": video_gcs_uri,
        "prompt": prompt_used,
        "timestamp": ingestion_time,
    })


def organize_storage_files(gtin: str, front_gcs_uri: str, back_gcs_uri: str, video_gcs_uri: str) -> tuple[str, str]:
    """Organizes the storage files by moving the source images and generated video to their respective folders."""
    input_asset_bucket: Bucket = storage_client.bucket(INPUT_GCS_BUCKET)
    processed_video_bucket: Bucket = storage_client.bucket(OUTPUT_GCS_BUCKET)

    # Process Front Image
    front_image_blob: Blob = Blob.from_uri(front_gcs_uri, client=storage_client)
    # Maintain folder structure or flatten? Flattening to GTIN folder as per previous logic.
    front_image_name: str = f"{gtin}/{front_gcs_uri.split('/')[-1]}"
    input_asset_bucket.copy_blob(
        blob=front_image_blob,
        destination_bucket=processed_video_bucket,
        new_name=front_image_name
    )
    front_image_blob.delete()
    
    # Process Back Image
    back_image_blob: Blob = Blob.from_uri(back_gcs_uri, client=storage_client)
    back_image_name: str = f"{gtin}/{back_gcs_uri.split('/')[-1]}"
    input_asset_bucket.copy_blob(
        blob=back_image_blob,
        destination_bucket=processed_video_bucket,
        new_name=back_image_name
    )
    back_image_blob.delete()

    print(f"Sources moved to processed bucket for GTIN {gtin}")

    generated_video_blob: Blob = Blob.from_uri(video_gcs_uri, client=storage_client)
    generated_video_name: str = f"{gtin}/{front_image_name.split('/')[-1].split('.')[0]}_{datetime.now().isoformat()}.mp4"
    processed_video_bucket.copy_blob(
        blob=generated_video_blob,
        destination_bucket=processed_video_bucket,
        new_name=generated_video_name
    )

    generated_video_blob.delete()

    print(f"Video moved after generating and stored at: gs://{OUTPUT_GCS_BUCKET}/{generated_video_name}")

    return f"gs://{OUTPUT_GCS_BUCKET}/{generated_video_name}", f"gs://{OUTPUT_GCS_BUCKET}/{front_image_name}"


def generate_video(gtin: str, category: str, front_image_gcs_uri: str, back_image_gcs_uri: str, mime_type: str, aspect_ratio: str) -> tuple[str, str]:
    """Generates a video from the given image pair using Subject References"""

    # specific prompt based on category
    prompt_text = PROMPT_MAPPING.get(category, DEFAULT_FALLBACK_PROMPT)
    print(f"Generating video for category '{category}' using prompt: {prompt_text}")

    operation: GenerateVideosOperation = genai_client.models.generate_videos(
        model=VEO_MODEL,

        # Prompt + Product References (Subject References)
        # Note: 'image' is set to None because we are NOT doing Image-to-Video (first frame).
        # We are doing Text-to-Video with Reference Images.
        prompt=prompt_text,
        subject_references=[
            Image(gcs_uri=front_image_gcs_uri, mime_type=mime_type),
            Image(gcs_uri=back_image_gcs_uri, mime_type=mime_type)
        ],

        config=GenerateVideosConfig(
            number_of_videos=1,
            duration_seconds=8,
            output_gcs_uri=f"gs://{OUTPUT_GCS_BUCKET}/{gtin}/",
            aspect_ratio=aspect_ratio,
            generate_audio=False,
            resolution="1080p"
        )
    )

    while not operation.done:
        print("Waiting for video generation to complete...")
        time.sleep(10)
        operation = genai_client.operations.get(operation)

    if operation.error:
        print(f"Video generation failed: {operation.error}")
        raise Exception(f"Video generation failed: {operation.error}")

    print("Video generation completed.")
    generated_video_response: GenerateVideosResponse | None = operation.response
    generated_videos: list[GeneratedVideo] | None = generated_video_response.generated_videos # type: ignore
    generated_video: GeneratedVideo = generated_videos[0]  # type: ignore

    return generated_video.video.uri, prompt_text  # type: ignore


def main(request) -> tuple[str, int]:
    data: dict = request.get_json(silent=True)

    gtin: str | None = data.get('gtin')
    category: str | None = data.get('category')
    front_image_gcs_uri: str | None = data.get('front_image_gcs_uri')
    back_image_gcs_uri: str | None = data.get('back_image_gcs_uri')
    
    mime_type: str | None = data.get('mime_type')
    aspect_ratio: str | None = data.get('aspect_ratio')

    assert gtin is not None, "gtin is required"
    assert category is not None, "category is required"
    assert front_image_gcs_uri is not None, "front_image_gcs_uri is required"
    assert back_image_gcs_uri is not None, "back_image_gcs_uri is required"
    assert mime_type is not None, "mime_type is required"
    assert aspect_ratio is not None, "aspect_ratio is required"

    try:
        video_gcs_uri, used_prompt = generate_video(
            gtin,
            category,
            front_image_gcs_uri,
            back_image_gcs_uri,
            mime_type,
            aspect_ratio
        )

    except Exception as exception:
        print(f"Error during video generation: {exception}")
        log_error(gtin, front_image_gcs_uri, str(exception))
        return str(exception), 500

    video_gcs_uri, final_image_uri = organize_storage_files(gtin, front_image_gcs_uri, back_image_gcs_uri, video_gcs_uri)
    log_success(gtin, final_image_uri, video_gcs_uri, used_prompt)

    return "OK", 200


# For local testing purposes. Run `python main.py`
if __name__ == '__main__':
    gtin: str = "2246065552629"
    category: str = "female_clothes"
    front_image: str = "gs://galeria-veo3-input-assets-galeria-retail-api-dev/female_clothes/2246065552629_09.webp"
    back_image: str = "gs://galeria-veo3-input-assets-galeria-retail-api-dev/female_clothes/2246065552629_10.webp"

    video_gcs_uri, prompt = generate_video(
        gtin=gtin,
        category=category,
        front_image_gcs_uri=front_image,
        back_image_gcs_uri=back_image,
        mime_type="image/webp",
        aspect_ratio="9:16"
    )

    video_gcs_uri, img_uri = organize_storage_files(gtin, front_image, back_image, video_gcs_uri)
    log_success(gtin, img_uri, video_gcs_uri, prompt)