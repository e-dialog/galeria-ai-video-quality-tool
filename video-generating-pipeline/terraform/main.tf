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

data "google_storage_project_service_account" "gcs_account" {
}

data "google_service_account" "terraform_service_agent" {
  account_id = "terraform-service-agent"
}

locals {
  firestore_database_name = "video-generation-tracking-db"
  video_generation_asset_bucket_name = "video-generation-assets-bucket-${var.project_id}"
}

