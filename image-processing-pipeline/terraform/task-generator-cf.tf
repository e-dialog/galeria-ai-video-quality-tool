resource "google_cloudfunctions2_function" "task_generator" {
  name        = local.task_generator_cf_name
  location    = local.region
  description = "GCS listener, BigQuery logger, and Cloud Tasks enqueuer."

  build_config {
    runtime     = "python312"
    entry_point = "main"
    service_account = data.google_service_account.terraform_service_agent.id

    source {
      storage_source {
        bucket = data.google_storage_bucket.terraform_state_bucket.name
        object = google_storage_bucket_object.task_generator_cf_bucket_object.name
      }
    }
  }

  service_config {
    service_account_email = local.image_processing_pipeline_sa_email
    ingress_settings      = "ALLOW_ALL"

    environment_variables = {
      PROJECT_ID                   = local.project_id
      TASK_QUEUE_NAME              = google_cloud_tasks_queue.provisioned_throughput_rate_limiter.name
      TASK_QUEUE_LOCATION          = google_cloud_tasks_queue.provisioned_throughput_rate_limiter.location
      BIGQUERY_VIDEO_LOGS_TABLE_ID = "${local.project_id}.image_processing_logs.galeria_veo3_video_ingestion_events"
    }
  }

  event_trigger {
    trigger_region = local.region
    event_type     = "google.cloud.storage.object.v1.finalized"

    event_filters {
      attribute = "bucket"
      value     = local.galeria_input_assets_bucket_name
    }

    # FIXME: Hardcoded to only listen to images in the models/ folder
    event_filters {
      attribute = "prefix"
      value     = local.models_only_prefix
    }
  }
}

data "archive_file" "task_generator_cf_archive" {
  type        = "zip"
  source_dir  = "${path.module}/../${local.task_generator_cf_name}/src"
  output_path = "${local.task_generator_cf_name}.zip"
}

resource "google_storage_bucket_object" "task_generator_cf_bucket_object" {
  name   = "artifacts/${local.task_generator_cf_name}.zip"
  bucket = data.google_storage_bucket.terraform_state_bucket.name
  source = "${local.task_generator_cf_name}.zip"
}
