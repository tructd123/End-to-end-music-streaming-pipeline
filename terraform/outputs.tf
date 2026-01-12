# Terraform Outputs

output "gcs_bucket_name" {
  description = "Name of the created GCS bucket"
  value       = google_storage_bucket.data_lake.name
}

output "gcs_bucket_url" {
  description = "URL of the GCS bucket"
  value       = google_storage_bucket.data_lake.url
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.music_streaming.dataset_id
}

output "service_account_email" {
  description = "Service Account email"
  value       = google_service_account.pipeline_sa.email
}

output "service_account_key_path" {
  description = "Path to Service Account key file"
  value       = local_file.sa_key_file.filename
}
