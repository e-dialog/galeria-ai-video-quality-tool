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
DEFAULT_MODEL_PROMPT: str = """
    A hyperrealistic, commercial e-commerce video with a minimalist, gallery aesthetic.
    
    **Subject:** 
    The video is focused entirely on a fashion garment, which is displayed by a figure. The primary subject is the garment itself, emphasizing its fabric texture, design, and silhouette. 
    
    **Environment:** 
    The background MUST be a perfectly clean, flat, shadowless, solid white studio environment. There are no other objects, props, or environmental details.
    
    **Motion:**
    The video begins with the camera holding perfectly still for one second on a static, centered, head-on view of the garment.
    The figure then begins a **glacial, almost imperceptible** turn to the right. The entire movement from the front view to the side profile must take **no less than 5 seconds** to complete, ensuring the motion is extremely slow and controlled.
    The movement **comes to a complete stop precisely when the garment is viewed from a perfect 20-degree side profile.**
    
    **Director's Note on Motion:** The camera MUST remain completely static, locked in place, with no panning, zooming, or shaking. The figure's turn is designed to reveal ONLY the garment's fit and drape from front to side. The final frame must be a true 20-degree profile view, **no more, no less.** Under no circumstances should any part of the garment's rear side be shown.
    **Lighting:** The lighting on the primary product must remain perfectly even, diffuse, and shadowless, consistent with professional studio lighting. The product should not be affected by the environmental lighting or cast any shadows onto the background.
    **Material & Texture Focus:** As the figure turns, the studio light must subtly catch and define the texture of the fabric. The goal is to make the material look tangible and high-quality.
    **Composition & Framing:** The framing must replicate the original input image's perspective perfectly. The garment is perfectly centered with ample, balanced negative space. There is absolutely **no zooming, reframing, or cropping** of the shot. The frame remains completely static, matching the initial perspective; only the figure moves within it.
    **Overall Mood & Tone:** The aesthetic is minimalist, sophisticated, calm, and premium. The final output should feel clean, airy, and trustworthy.
    
    **Strict Constraints:**
    - The camera must be static. There is absolutely no movement.
    - The figure's turn must be extremely slow and smooth.
    - The figure's facial expression must remain neutral and consistent throughout.
    - **The back of the clothing must never be shown, rendered, or generated.** The view is strictly limited to the front and front-quarter.
    - The video must be a single, uninterrupted shot.
    - The white background and lighting must remain constant.
    - No black bars above or beside the video
"""

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
        api_version="v1",
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
        "prompt": DEFAULT_MODEL_PROMPT,
        "notes": notes,
        "timestamp": ingestion_time,
    })


def log_success(gtin: str, image_gcs_uri: str, video_gcs_uri: str) -> None:
    """Logs the video generation event to BigQuery."""
    ingestion_time: str = datetime.now().isoformat()

    log({
        "gtin": gtin,
        "status": "VIDEO_GENERATION_COMPLETED",
        "image_gcs_uri": image_gcs_uri,
        "video_gcs_uri": video_gcs_uri,
        "prompt": DEFAULT_MODEL_PROMPT,
        "timestamp": ingestion_time,
    })


def organize_storage_files(gtin: str, image_gcs_uri: str, video_gcs_uri: str) -> tuple[str, str]:
    """Organizes the storage files by moving the source image and generated video to their respective folders."""
    input_asset_bucket: Bucket = storage_client.bucket(INPUT_GCS_BUCKET)
    processed_video_bucket: Bucket = storage_client.bucket(OUTPUT_GCS_BUCKET)

    source_image_blob: Blob = Blob.from_uri(image_gcs_uri, client=storage_client)
    source_image_name: str = f"{gtin}/{image_gcs_uri.split('/')[-1]}"
    input_asset_bucket.copy_blob(
        blob=source_image_blob,
        destination_bucket=processed_video_bucket,
        new_name=source_image_name
    )

    source_image_blob.delete()

    print(f"Source moved to processed bucket at: gs://{OUTPUT_GCS_BUCKET}/{source_image_name}")

    generated_video_blob: Blob = Blob.from_uri(video_gcs_uri, client=storage_client)
    generated_video_name: str = f"{gtin}/{source_image_name.split('/')[-1].split('.')[0]}_{datetime.now().isoformat()}.mp4"
    processed_video_bucket.copy_blob(
        blob=generated_video_blob,
        destination_bucket=processed_video_bucket,
        new_name=generated_video_name
    )

    generated_video_blob.delete()

    print(f"Video moved after generating and stored at: gs://{OUTPUT_GCS_BUCKET}/{generated_video_name}")

    return f"gs://{OUTPUT_GCS_BUCKET}/{generated_video_name}", f"gs://{OUTPUT_GCS_BUCKET}/{source_image_name}"


def generate_video(gtin: str, image_gcs_uri: str, mime_type: str, aspect_ratio: str) -> str:
    """Generates a video from the given image"""

    operation: GenerateVideosOperation = genai_client.models.generate_videos(
        model=VEO_MODEL,

        source=GenerateVideosSource(
            prompt=DEFAULT_MODEL_PROMPT,
            image=Image(gcs_uri=image_gcs_uri, mime_type=mime_type)
        ),

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

    return generated_video.video.uri  # type: ignore


def main(request) -> tuple[str, int]:
    data: dict = request.get_json(silent=True)

    gtin: str | None = data.get('gtin')
    image_gcs_uri: str | None = data.get('image_gcs_uri')
    mime_type: str | None = data.get('mime_type')
    aspect_ratio: str | None = data.get('aspect_ratio')

    assert gtin is not None, "gtin is required"
    assert image_gcs_uri is not None, "image_gcs_uri is required"
    assert mime_type is not None, "mime_type is required"
    assert aspect_ratio is not None, "aspect_ratio is required"

    try:
        video_gcs_uri: str = generate_video(
            gtin,
            image_gcs_uri,
            mime_type,
            aspect_ratio
        )

    except Exception as exception:
        print(f"Error during video generation: {exception}")
        log_error(gtin, image_gcs_uri, "VIDEO_GENERATION_FAILED")
        return str(exception), 500

    video_gcs_uri, image_gcs_uri = organize_storage_files(gtin, image_gcs_uri, video_gcs_uri)
    log_success(gtin, image_gcs_uri, video_gcs_uri)

    return "OK", 200


# For local testing purposes. Run `python main.py`
if __name__ == '__main__':
    gtin: str = "2246065552629"
    image_gcs_uri: str = "gs://galeria-veo3-input-assets-galeria-retail-api-dev/models/2246065552629_09.webp"

    video_gcs_uri: str = generate_video(
        gtin=gtin,
        image_gcs_uri=image_gcs_uri,
        mime_type="image/webp",
        aspect_ratio="9:16"
    )

    video_gcs_uri, image_gcs_uri = organize_storage_files(gtin, image_gcs_uri, video_gcs_uri)
    log_success(gtin, image_gcs_uri, video_gcs_uri)
