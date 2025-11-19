import time
import os
import json
from datetime import datetime
from google.cloud import bigquery, storage
from google.oauth2 import service_account
import google.generativeai as genai

# --- Configuration ---
BUCKET_NAME = "galeria-retail-api-dev-moving-images"
BIGQUERY_TABLE = "galeria-retail-api-dev.moving_images.overview"
OUTPUT_PREFIX = "ai-video-quality-tool/output/"
POLL_INTERVAL = 10  # Seconds to wait if no work found
GENERATION_COOLDOWN = 31  # Seconds to wait after a generation

def get_gcp_clients():
    """Initializes GCP clients."""
    try:
        key_json = os.getenv("SERVICE_ACCOUNT_KEY_JSON")
        if key_json:
            credentials_info = json.loads(key_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            storage_client = storage.Client(credentials=credentials)
            bigquery_client = bigquery.Client(credentials=credentials)
        else:
            storage_client = storage.Client()
            bigquery_client = bigquery.Client()
        return storage_client, bigquery_client
    except Exception as e:
        print(f"Failed to initialize GCP clients: {e}")
        return None, None

def get_next_pending_job(bq_client):
    """Fetches the next PENDING job from BigQuery."""
    query = f"""
        SELECT image_id, prompt, source_gcs_path
        FROM `{BIGQUERY_TABLE}`
        WHERE generation_status = 'PENDING'
        ORDER BY last_updated ASC
        LIMIT 1
    """
    try:
        results = bq_client.query(query).result()
        for row in results:
            return dict(row)
    except Exception as e:
        print(f"Error querying BigQuery: {e}")
    return None

def update_job_status(bq_client, image_id, status, video_path=None):
    """Updates the job status in BigQuery."""
    query = f"""
        UPDATE `{BIGQUERY_TABLE}`
        SET 
            generation_status = @status,
            video_id = @video_path,
            last_updated = @timestamp
        WHERE image_id = @image_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("video_path", "STRING", video_path),
            bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", datetime.utcnow().isoformat()),
            bigquery.ScalarQueryParameter("image_id", "STRING", image_id),
        ]
    )
    try:
        bq_client.query(query, job_config=job_config).result()
        print(f"Updated {image_id} status to {status}")
    except Exception as e:
        print(f"Failed to update status for {image_id}: {e}")

def generate_video(prompt, output_path):
    """
    Generates a video using the Veo API.
    This is a placeholder for the actual Veo API call using google-generativeai.
    """
    print(f"Generating video for prompt: {prompt[:50]}...")
    
    # TODO: Implement actual Veo API call here
    # model = genai.GenerativeModel('veo-3.1-generate-preview')
    # operation = model.generate_video(prompt=prompt)
    # video_content = operation.result() 
    
    # For now, we will simulate generation by sleeping and creating a dummy file
    time.sleep(5) 
    return b"DUMMY_VIDEO_CONTENT"

def upload_to_gcs(storage_client, content, destination_blob_name):
    """Uploads video content to GCS."""
    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(content, content_type='video/mp4')
        print(f"Uploaded video to gs://{BUCKET_NAME}/{destination_blob_name}")
        return True
    except Exception as e:
        print(f"Failed to upload to GCS: {e}")
        return False

def main():
    print("Starting Video Generation Worker...")
    storage_client, bq_client = get_gcp_clients()
    
    if not storage_client or not bq_client:
        print("Could not initialize clients. Exiting.")
        return

    while True:
        job = get_next_pending_job(bq_client)
        
        if job:
            image_id = job['image_id']
            prompt = job['prompt']
            print(f"Found job: {image_id}")
            
            # 1. Mark as GENERATING
            update_job_status(bq_client, image_id, 'GENERATING')
            
            try:
                # 2. Generate Video
                video_content = generate_video(prompt, None)
                
                # 3. Upload to GCS
                # Strip existing extension if present to avoid double extension (e.g. .jpg.mp4)
                stem = os.path.splitext(image_id)[0]
                filename = f"{stem}.mp4"
                gcs_path = f"{OUTPUT_PREFIX}{filename}"
                
                if upload_to_gcs(storage_client, video_content, gcs_path):
                    # 4. Mark as APPROVAL_PENDING
                    update_job_status(bq_client, image_id, 'APPROVAL_PENDING', video_path=gcs_path)
                else:
                    # Upload failed
                    update_job_status(bq_client, image_id, 'FAILED')

            except Exception as e:
                print(f"Generation failed for {image_id}: {e}")
                update_job_status(bq_client, image_id, 'FAILED')
            
            # 5. Cooldown
            print(f"Sleeping for {GENERATION_COOLDOWN} seconds...")
            time.sleep(GENERATION_COOLDOWN)
            
        else:
            print(f"No pending jobs. Sleeping for {POLL_INTERVAL} seconds...")
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
