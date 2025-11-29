resource "google_cloudfunctions2_function" "video_converter" {
  name        = local.video_converter_cf_name
  location    = local.region
  description = "Converts Veo3 generated mp4 videos to webm format for better compatibility and performance."

  build_config {
    runtime         = "python312"
    entry_point     = "main"
    service_account = data.google_service_account.terraform_service_agent.id

    source {
      storage_source {
        bucket = data.google_storage_bucket.terraform_state_bucket.name
        object = google_storage_bucket_object.video_converter_cf_bucket_object.name
      }
    }
  }

  service_config {
    service_account_email = local.image_processing_pipeline_sa_email
    ingress_settings      = "ALLOW_INTERNAL_ONLY"
    available_memory      = "1024M"
    available_cpu         = "2"
    timeout_seconds       = 120

    environment_variables = {
      APPROVED_GCS_BUCKET = local.galeria_approved_assets_bucket_name
    }

    # With video generation taking 30s-60s and us limiting to one query per 32 seconds, we can technically have 2 instances running at any given time
    max_instance_count = 2
  }

  event_trigger {
    trigger_region = local.region
    event_type   = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic = google_pubsub_topic.video_approved_notifications.id
    retry_policy = "RETRY_POLICY_RETRY"
  }
}

resource "google_pubsub_topic" "video_approved_notifications" {
  name = "video-approved-notifications"
}

# This is the trigger for uploads to the approved assets bucket
resource "google_storage_notification" "approved_asset_bucket" {
  bucket         = google_storage_bucket.galeria_approved_assets_bucket.name
  payload_format = "JSON_API_V1"
  topic          = google_pubsub_topic.video_approved_notifications.id
  event_types    = ["OBJECT_FINALIZE"]
}

data "archive_file" "video_converter_cf_archive" {
  type        = "zip"
  source_dir  = "${path.module}/../${local.video_converter_cf_name}/src"
  output_path = "${local.video_converter_cf_name}.zip"
}

resource "google_storage_bucket_object" "video_converter_cf_bucket_object" {
  name   = "${local.video_converter_cf_name}/${data.archive_file.video_converter_cf_archive.output_md5}.zip"
  bucket = data.google_storage_bucket.terraform_state_bucket.name
  source = "${local.video_converter_cf_name}.zip"
}
