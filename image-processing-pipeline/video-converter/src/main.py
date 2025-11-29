import base64
import json
import math
import os
import tempfile

from google.cloud.storage import Blob, Bucket
from google.cloud.storage import Client as StorageClient
from moviepy import VideoFileClip

# 50MP limit. We use 49M to be safe.
MAX_TOTAL_PIXELS: int = 49_999_900

# 12 FPS is the sweet spot for 8-second videos to maintain resolution
TARGET_FPS: int = 24

APPROVED_GCS_BUCKET: str | None = os.getenv("APPROVED_GCS_BUCKET")
assert APPROVED_GCS_BUCKET is not None, "APPROVED_GCS_BUCKET environment variable is required"

storage_client: StorageClient = StorageClient()
approved_bucket: Bucket = storage_client.bucket(APPROVED_GCS_BUCKET)

def unpack_event_message(event) -> dict:
    """Unpacks the Pub/Sub message and returns the email content as a dictionary"""
    data: str = base64.b64decode(event['data']).decode('utf-8')
    return json.loads(data)

def convert_mp4_to_webp_gcs(source_blob_name: str, target_blob_name: str) -> None:
    source_blob: Blob = approved_bucket.blob(source_blob_name)
    

    with tempfile.NamedTemporaryFile(suffix=".mp4") as temp_input, \
        tempfile.NamedTemporaryFile(suffix=".webp") as temp_output:
    
        print(f"Downloading {source_blob_name}...")
        # Download strictly to disk (low memory usage)
        source_blob.download_to_filename(temp_input.name)

        print("Converting...")
        try:
            with VideoFileClip(temp_input.name) as clip:
                # 1. Enforce Safety Check
                # Even if we know it's 8s, we calculate to be safe against 8.5s or 9s videos
                total_frames = clip.duration * TARGET_FPS
                current_pixels = clip.w * clip.h * total_frames
                
                if current_pixels > MAX_TOTAL_PIXELS:
                    # Calculate new max width maintaining aspect ratio
                    max_pixels_per_frame = MAX_TOTAL_PIXELS / total_frames
                    scale = math.sqrt(max_pixels_per_frame / (clip.w * clip.h))
                    
                    new_w = int(clip.w * scale)
                    # Ensure width is even
                    if new_w % 2 != 0: new_w -= 1
                    
                    print(f"8s Constraint: Downscaling to width={new_w}px")
                    target_blob_name = target_blob_name.replace('.webp', f'_{TARGET_FPS}fps_w{new_w}.webp')
                    clip = clip.resized(width=new_w)
                
                    # 2. Convert
                    clip.write_videofile( # type: ignore
                        temp_output.name,
                        fps=TARGET_FPS, 
                        codec='libwebp',
                        audio=False,
                        logger=None,
                        ffmpeg_params=[
                            "-lossless", "0",
                            "-compression_level", "4",
                            "-q:v", "75", 
                            "-loop", "0",
                            "-preset", "default"
                        ]
                    )

            print(f"Uploading to {target_blob_name}...")
            # Upload directly from disk
            
            target_blob: Blob = approved_bucket.blob(target_blob_name)
            target_blob.upload_from_filename(temp_output.name, content_type='image/webp')

        except Exception as e:
            print(f"Error converting {source_blob_name}: {e}")
            raise


def main(event, context):
    """Background Cloud Function to convert mp4 videos to webp format upon upload."""
    data: dict = unpack_event_message(event)
    
    source_blob_name: str = data['name']
    if source_blob_name.startswith('!production/'):
        # Skip already processed files
        return "OK", 200
    
    gtin: str = source_blob_name.split('/')[0]
    print(f"Processing file: {source_blob_name}")

    target_blob_name: str = f"!production/{gtin}.webp"
    convert_mp4_to_webp_gcs(source_blob_name, target_blob_name)
    
    return "OK", 200
    
    
if __name__ == "__main__":
    # For local testing purposes
    source_blob_name: str = "4062742300097/sample_0_2025-11-28T06:30:39.849686.mp4"
    
    gtin: str = source_blob_name.split('/')[0]
    print(f"Processing file: {source_blob_name}")

    target_blob_name: str = f"!production/{gtin}.webp"
    convert_mp4_to_webp_gcs(source_blob_name, target_blob_name)