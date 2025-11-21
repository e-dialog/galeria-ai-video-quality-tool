resource "google_service_account" "image_processing_pipeline" {
    account_id = "image-processing-pipeline-sa"
    display_name = "Image Processing Pipeline Service Account"
}

resource "google_cloud_run_service_iam_member" "cloudtasks_video_generator_invoker" {
  location = google_cloudfunctions2_function.video_generator.location
  service  = google_cloudfunctions2_function.video_generator.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:service-${local.project_number}@gcp-sa-cloudtasks.iam.gserviceaccount.com"
}

resource "google_pubsub_topic_iam_binding" "gcs_service_agent_publisher_role" {
  topic   = google_pubsub_topic.new_asset_notification_topic.id
  role    = "roles/pubsub.publisher"
  members = ["serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"]
}