import os
import time

from google.genai import Client as GenAIClient
from google.genai.types import (GeneratedVideo, GenerateVideosConfig,
                                GenerateVideosOperation,
                                GenerateVideosResponse, HttpOptions, Image,
                                VideoGenerationReferenceImage,
                                VideoGenerationReferenceType)

# Static constants
VEO_MODEL: str = "veo-3.1-generate-preview"
PROMPT_MAPPING: dict[str, str] = {
    "female_clothes": "Generate a fashion studio shot of the attached female clothing. The woman in the video is wearing this clothing and is visible from the front before making a casual 180 degree turn to show the clothing from the back.",
    "female_underwear": "Generate a fashion studio shot of the attached female underwear. The woman in the video is wearing this underwear and is visible from the front before making a casual 180 degree turn to show the underwear from the back.",
    "male_clothes": "Generate a fashion studio shot of the attached male clothing. The man in the video is wearing this clothing and is visible from the front before making a casual 180 degree turn to show the clothing from the back.",
    "male_underwear": "Generate a fashion studio shot of the attached male underwear. The man in the video is wearing this underwear and is visible from the front before making a casual 180 degree turn to show the underwear from the back."
}

# Fallback if category isn't matched
DEFAULT_FALLBACK_PROMPT: str = "Generate a fashion studio shot of the attached product. The person in the video is wearing this product and is visible from the front before making a casual 180 degree turn to show the product from the back."

# Environment variables
PROJECT_ID: str | None = os.getenv("PROJECT_ID")
PROCESSED_GCS_BUCKET: str | None = os.getenv("PROCESSED_GCS_BUCKET")

assert PROJECT_ID is not None, "PROJECT_ID environment variable is required"
assert PROCESSED_GCS_BUCKET is not None, "PROCESSED_GCS_BUCKET environment variable is required"

genai_client: GenAIClient = GenAIClient(
    vertexai=True,
    project=PROJECT_ID,
    location='us-central1',
    http_options=HttpOptions(
        api_version="v1",  # type: ignore
        headers={
            # Important! This ensures we do not overshoot on the provisioned throughput capacity
            # https://docs.cloud.google.com/vertex-ai/generative-ai/docs/provisioned-throughput/use-provisioned-throughput#only-provisioned-throughput
            "X-Vertex-AI-LLM-Request-Type": "dedicated"
        }
    )
)


def get_mime_type(gcs_uri: str) -> str:
    """Returns the MIME type based on the file extension."""
    if gcs_uri.endswith('.webp'):
        return 'image/webp'
    elif gcs_uri.endswith('.png'):
        return 'image/png'
    elif gcs_uri.endswith('.jpg') or gcs_uri.endswith('.jpeg'):
        return 'image/jpeg'
    else:
        raise ValueError(f"Unsupported image format for URI: {gcs_uri}")


def generate_video(gtin: str, category: str, gcs_uris: list[str]) -> tuple[str, str]:
    """Generates a video using the given images as subject references."""

    # specific prompt based on category
    prompt_text = PROMPT_MAPPING.get(category, DEFAULT_FALLBACK_PROMPT)
    print(f"Generating video for gtin '{gtin}' with category '{category}'")

    operation: GenerateVideosOperation = genai_client.models.generate_videos(
        model=VEO_MODEL,

        # Prompt + Product References (Subject References)
        # Note: 'image' is set to None because we are NOT doing Image-to-Video (first frame).
        # We are doing Text-to-Video with Reference Images.
        prompt=prompt_text,
        config=GenerateVideosConfig(
            number_of_videos=1,
            reference_images=[
                VideoGenerationReferenceImage(
                    image=Image(
                        gcs_uri=gcs_uri,
                        mime_type=get_mime_type(gcs_uri)
                    ),
                    reference_type=VideoGenerationReferenceType.ASSET
                )
                for gcs_uri in gcs_uris
            ],
            duration_seconds=8,
            output_gcs_uri=f"gs://{PROCESSED_GCS_BUCKET}/{gtin}/",
            generate_audio=False,
            resolution="1080p"
        )
    )

    while not operation.done:
        time.sleep(5)
        operation = genai_client.operations.get(operation)

    if operation.error:
        print(f"Video generation failed: {operation.error}")
        raise Exception(f"Video generation failed: {operation.error}")

    print("Video generation completed.")
    generated_video_response: GenerateVideosResponse | None = operation.response
    generated_videos: list[GeneratedVideo] | None = generated_video_response.generated_videos # type: ignore
    generated_video: GeneratedVideo = generated_videos[0]  # type: ignore

    return generated_video.video.uri, prompt_text  # type: ignore
