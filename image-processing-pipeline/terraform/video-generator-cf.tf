# TODO: Update the source code bucket and object names

resource "google_cloudfunctions2_function" "video_generator" {
  name        = local.video_generator_cf_name
  location    = local.region
  description = "Performs rate-limited Veo 3 video generation."

  build_config {
    runtime     = "python312"
    entry_point = "main"

    source {
      storage_source {
        bucket = data.google_storage_bucket.terraform_state_bucket.name
        object = google_storage_bucket_object.video_generator_cf_bucket_object.name
      }
    }
  }

  service_config {
    service_account_email = local.image_processing_pipeline_sa_email
    ingress_settings      = "ALLOW_INTERNAL_ONLY"
    available_memory      = "256Mi"
    timeout_seconds       = 3600

    environment_variables = {
      PROJECT_NUMBER            = local.project_number
      PROJECT_ID                = local.project_id
      OUTPUT_GCS_BUCKET         = local.galeria_processed_assets_bucket_name
      BIGQUERY_VIDEO_LOGS_TABLE_ID = "${local.project_id}.image_processing_logs.galeria_veo3_video_ingestion_events"
    }

    # With video generation taking 30s-60s and us limiting to one query per 32 seconds, we can technically have 2 instances running at any given time
    max_instance_count = 2
  }
}

data "archive_file" "video_generator_cf_archive" {
  type        = "zip"
  source_dir  = "${path.module}/../${local.video_generator_cf_name}/src"
  output_path = "${local.video_generator_cf_name}.zip"
}

resource "google_storage_bucket_object" "video_generator_cf_bucket_object" {
  name   = "artifacts/${local.video_generator_cf_name}.zip"
  bucket = data.google_storage_bucket.terraform_state_bucket.name
  source = "${local.video_generator_cf_name}.zip"
}
