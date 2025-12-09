# resource "google_service_account" "image_processing_pipeline" {
#   account_id   = "image-processing-pipeline-sa"
#   display_name = "Image Processing Pipeline Service Account"
# }

# resource "google_cloud_run_service_iam_member" "cloudtasks_video_generator_invoker" {
#   location = google_cloudfunctions2_function.video_generator.location
#   service  = google_cloudfunctions2_function.video_generator.name
#   role     = "roles/run.invoker"
#   member   = "serviceAccount:service-${local.project_number}@gcp-sa-cloudtasks.iam.gserviceaccount.com"
# }

# resource "google_pubsub_topic_iam_binding" "gcs_service_agent_new_asset_notification_topic_publisher_role" {
#   topic   = google_pubsub_topic.new_asset_notification_topic.id
#   role    = "roles/pubsub.publisher"
#   members = ["serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"]
# }

# resource "google_pubsub_topic_iam_binding" "gcs_service_agent_video_approved_notifications_topic_publisher_role" {
#   topic   = google_pubsub_topic.video_approved_notifications.id
#   role    = "roles/pubsub.publisher"
#   members = ["serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"]
# }

# resource "google_project_iam_member" "image_processing_pipeline_impersonate_task_sa" {
#   project = local.project_id
#   role    = "roles/iam.serviceAccountUser"
#   member  = google_service_account.image_processing_pipeline.member
# }

# resource "google_project_iam_member" "image_processing_pipeline_storage_admin" {
#   project = local.project_id
#   role    = "roles/storage.admin"
#   member  = google_service_account.image_processing_pipeline.member
# }

# resource "google_project_iam_member" "image_processing_pipeline_bigquery_data_admin" {
#   project = local.project_id
#   role    = "roles/bigquery.admin"
#   member  = google_service_account.image_processing_pipeline.member
# }

# resource "google_project_iam_member" "image_processing_pipeline_cloud_tasks_enqueuer" {
#   project = local.project_id
#   role    = "roles/cloudtasks.enqueuer"
#   member  = google_service_account.image_processing_pipeline.member
# }

# resource "google_project_iam_member" "image_processing_pipeline_cloud_functions_invoker" {
#   project = local.project_id
#   role    = "roles/cloudfunctions.invoker"
#   member  = google_service_account.image_processing_pipeline.member
# }

# resource "google_project_iam_member" "image_processing_pipeline_cloud_run_invoker" {
#   project = local.project_id
#   role    = "roles/run.invoker"
#   member  = google_service_account.image_processing_pipeline.member
# }

# resource "google_project_iam_member" "image_processing_pipeline_vertex_ai_user" {
#   project = local.project_id
#   role    = "roles/aiplatform.user"
#   member  = google_service_account.image_processing_pipeline.member
# }
