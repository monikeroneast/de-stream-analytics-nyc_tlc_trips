output "data_lake_bucket_name" {
  description = "The name of the GCS data lake bucket"
  value       = google_storage_bucket.data_lake.name
}

output "bigquery_dataset_id" {
  description = "The BigQuery dataset ID"
  value       = google_bigquery_dataset.taxi_dataset.dataset_id
}

output "bigquery_table_id" {
  description = "The BigQuery raw table ID"
  value       = google_bigquery_table.yellow_taxi_raw.table_id
}