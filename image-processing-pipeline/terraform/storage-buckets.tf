resource "google_storage_bucket" "galeria_input_assets_bucket" {
  name                        = local.galeria_input_assets_bucket_name
  location                    = "EU"
  force_destroy               = false
  uniform_bucket_level_access = true

  autoclass {
    enabled = true
  }
}

resource "google_storage_bucket" "galeria_processed_assets_bucket" {
  name                        = local.galeria_processed_assets_bucket_name
  location                    = "EU"
  force_destroy               = false
  uniform_bucket_level_access = true

  autoclass {
    enabled = true
  }
}

resource "google_storage_bucket" "galeria_approved_assets_bucket" {
  name                        = local.galeria_approved_assets_bucket_name
  location                    = "EU"
  force_destroy               = false
  uniform_bucket_level_access = true

  autoclass {
    enabled = true
  }
}

resource "google_storage_bucket" "general_input_assets_bucket" {
  name                        = local.general_input_assets_bucket_name
  location                    = "EU"
  force_destroy               = false
  uniform_bucket_level_access = true

  autoclass {
    enabled = true
  }
}

resource "google_storage_bucket" "general_processed_assets_bucket" {
  name                        = local.general_processed_assets_bucket_name
  location                    = "EU"
  force_destroy               = false
  uniform_bucket_level_access = true

  autoclass {
    enabled = true
  }
}

