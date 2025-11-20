"""
Video generation module using Google GenAI VEO model.
1. Generates a video from a single image using GenAI VEO model.
2. Uploads the generated video to a specified GCS bucket.
3. Logs the video generation event to BigQuery.
"""

import os
import time
from datetime import datetime

from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.storage import Blob, Bucket
from google.cloud.storage import Client as StorageClient
from google.genai import Client as GenAIClient
from google.genai.types import (GeneratedVideo, GenerateVideosConfig,
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
OUTPUT_GCS_BUCKET: str | None = os.getenv("OUTPUT_GCS_BUCKET")
BIGQUERY_VIDEO_LOGS_TABLE_ID: str | None = os.environ.get("BIGQUERY_VIDEO_LOGS_TABLE_ID")

assert PROJECT_NUMBER is not None, "PROJECT_NUMBER environment variable is required"
assert PROJECT_ID is not None, "PROJECT_ID environment variable is required"
assert OUTPUT_GCS_BUCKET is not None, "OUTPUT_GCS_BUCKET environment variable is required"
assert BIGQUERY_VIDEO_LOGS_TABLE_ID is not None, "BIGQUERY_VIDEO_LOGS_TABLE_ID environment variable is required"

# Google Client libraries
bigquery_client: BigQueryClient = BigQueryClient()

genai_client: GenAIClient = GenAIClient(
    vertexai=True,
    project=PROJECT_ID,
    location='europe-west4'
)

storage_client: StorageClient = StorageClient()


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


def generate_video(gtin: str, image_gcs_uri: str) -> str:
    """Generates a video from the given image"""

    operation: GenerateVideosOperation = genai_client.models.generate_videos(
        model=VEO_MODEL,

        source=GenerateVideosSource(
            prompt=DEFAULT_MODEL_PROMPT,
            image=Image(gcs_uri=image_gcs_uri)
        ),

        config=GenerateVideosConfig(
            number_of_videos=1,
            duration_seconds=8,
            output_gcs_uri=f"gs://{OUTPUT_GCS_BUCKET}/{gtin}/generated_videos/001.mp4",
        )
    )

    while not operation.done:
        print("Waiting for video generation to complete...")
        time.sleep(10)
        operation = genai_client.operations.get(operation)

    generated_video_response: GenerateVideosResponse | None = operation.response
    if generated_video_response is not None:
        generated_videos: list[GeneratedVideo] | None = generated_video_response.generated_videos

        if generated_videos is not None:
            print("Video generation completed successfully.")
            generated_video = generated_videos[0]
            
            print(generated_video.video.uri) # type: ignore

            # Upload the generated video to GCS
            # Move the original image to the processed bucket

    return f"gs://{OUTPUT_GCS_BUCKET}/{gtin}/generated_videos/001.mp4"


def main(request):
    data = request.get_json()

    gtin: str | None = data.get('gtin')
    image_gcs_uri: str | None = data.get('image_gcs_uri')
    aspect_ratio: str | None = data.get('aspect_ratio')

    assert gtin is not None, "gtin is required"
    assert image_gcs_uri is not None, "image_gcs_uri is required"
    assert aspect_ratio is not None, "aspect_ratio is required"

    video_gcs_uri: str = generate_video(gtin, image_gcs_uri)
    log(gtin, image_gcs_uri)


if __name__ == '__main__':
    generate_video(
        gtin="test-image-001",
        image_gcs_uri="gs://your-bucket/path/to/image.jpg"
    )
