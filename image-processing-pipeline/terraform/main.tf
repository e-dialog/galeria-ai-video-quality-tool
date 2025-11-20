terraform {
  required_version = ">=1.12"

  backend "gcs" {}
}

provider "google" {
  project = var.project_id
  region  = var.default_region
}

data "google_project" "project" {}

data "google_storage_bucket" "terraform_state_bucket" {
  name = "terraform-${var.project_id}"
}

locals {
  region     = var.default_region
  project_id = var.project_id

  galeria_input_assets_bucket_name     = "galeria-veo3-input-assets-${local.project_id}"
  galeria_processed_assets_bucket_name = "galeria-veo3-processed-assets-${local.project_id}"

  general_input_assets_bucket_name     = "general-veo3-input-assets-${local.project_id}"
  general_processed_assets_bucket_name = "general-veo3-processed-assets-${local.project_id}"

  models_only_prefix = "models/"

  # Calculate Max Dispatch Rate: 1 query / 31 seconds ≈ 0.032258 tasks/sec
  # We set it to 0.032 to be safe
  max_dispatch_rate = 0.032

  # Function names
  task_generator_cf_name  = "task-generator"
  video_generator_cf_name = "video-generator"

  # Service Accounts
  project_number                     = data.google_project.project.number
  image_processing_pipeline_sa_email = google_service_account.image_processing_pipeline.email
}
