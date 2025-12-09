# resource "google_cloud_tasks_queue" "provisioned_throughput_rate_limiter" {
#   name     = "provisioned-throughput-rate-limiter"
#   location = local.region

#   rate_limits {
#     max_dispatches_per_second = local.max_dispatch_rate
#     max_concurrent_dispatches = 100
#   }

#   retry_config {
#     max_attempts  = 5
#     max_backoff   = "3600s"
#     min_backoff   = "10s"
#     max_doublings = 5
#   }

#   # We override all tasks with a set URI target
#   http_target {
#     http_method = "POST"

#     uri_override {
#       scheme = "HTTPS"
#       host   = replace(google_cloudfunctions2_function.video_generator.service_config[0].uri, "https://", "")
#     }

#     oidc_token {
#       service_account_email = local.image_processing_pipeline_sa_email
#       audience              = google_cloudfunctions2_function.video_generator.service_config[0].uri
#     }
#   }

#   depends_on = [ google_cloudfunctions2_function.video_generator ]
# }

# resource "google_project_iam_member" "task_generator_cloudtasks_enqueuer_role" {
#   project = local.project_id
#   role    = "roles/cloudtasks.enqueuer"
#   member  = "serviceAccount:${local.image_processing_pipeline_sa_email}"
# }
