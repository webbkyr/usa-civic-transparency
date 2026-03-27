output "bucket_name" {
  description = "GCS bucket name"
  value       = google_storage_bucket.raw.name
}

output "raw_dataset_id" {
  value = google_bigquery_dataset.raw.dataset_id
}

output "staging_dataset_id" {
  value = google_bigquery_dataset.staging.dataset_id
}

output "reports_dataset_id" {
  value = google_bigquery_dataset.reports.dataset_id
}
