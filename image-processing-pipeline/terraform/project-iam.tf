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