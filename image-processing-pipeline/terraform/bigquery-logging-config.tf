# resource "google_bigquery_dataset" "image_processing_logs" {
#   dataset_id = "image_processing_logs"
#   location   = "EU"
#   description = "Dataset for logging image processing pipeline events"
# }

# resource "google_bigquery_table" "ingestion_events_table" {
#   dataset_id = google_bigquery_dataset.image_processing_logs.dataset_id
#   table_id   = "galeria_veo3_video_ingestion_events"
#   description = "Table for logging video ingestion events in the image processing pipeline triggered by GCS uploads to the new assets bucket for Galeria VEO3."
  
#   schema = file("${path.module}/bigquery/image-processing-logs/galeria-veo3-video-ingestion-events-schema.json")
#   clustering = [ "gtin" ]
# }