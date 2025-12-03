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
