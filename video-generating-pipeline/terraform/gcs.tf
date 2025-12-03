resource "google_storage_bucket" "video_generation_asset_bucket" {
  name                        = local.video_generation_asset_bucket_name
  location                    = "EU"
  force_destroy               = false
  uniform_bucket_level_access = true

  autoclass {
    enabled = true
  }
}

resource "google_storage_bucket_object" "uploads_folder" {
  name    = "uploads/"
  content = "-"
  bucket  = google_storage_bucket.video_generation_asset_bucket.name
}

resource "google_pubsub_topic" "image_uploads_topic" {
  name = "image-upload-notifications"
}

resource "google_storage_notification" "video_generation_asset_bucket_uploads_notification" {
  bucket = google_storage_bucket.video_generation_asset_bucket.name
  topic  = google_pubsub_topic.image_uploads_topic.id

  payload_format     = "JSON_API_V1"
  event_types        = ["OBJECT_FINALIZE"]
  object_name_prefix = "uploads/"

  depends_on = [google_pubsub_topic_iam_binding.image_uploads_topic_publisher_binding]
}

// Enable notifications by giving the correct IAM permission to the unique service account.
data "google_storage_project_service_account" "gcs_service_agent" { }

resource "google_pubsub_topic_iam_binding" "image_uploads_topic_publisher_binding" {
  topic   = google_pubsub_topic.image_uploads_topic.id
  role    = "roles/pubsub.publisher"
  members = ["serviceAccount:${data.google_storage_project_service_account.gcs_service_agent.email_address}"]
}
