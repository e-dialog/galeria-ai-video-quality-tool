resource "google_firestore_database" "database" {
  project     = var.project_id
  name        = local.firestore_database_name
  location_id = "eur3"
  type        = "FIRESTORE_NATIVE"
}