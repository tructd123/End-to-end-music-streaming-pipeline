# Terraform Outputs for SoundFlow Pipeline

# ============================================
# GCS Outputs
# ============================================
output "gcs_bucket_name" {
  description = "Name of the GCS data lake bucket"
  value       = google_storage_bucket.data_lake.name
}

output "gcs_bucket_url" {
  description = "URL of the GCS bucket"
  value       = google_storage_bucket.data_lake.url
}

output "gcs_raw_path" {
  description = "GCS path for raw data"
  value       = "gs://${google_storage_bucket.data_lake.name}/raw"
}

# ============================================
# BigQuery Outputs
# ============================================
output "bigquery_datasets" {
  description = "BigQuery dataset IDs"
  value = {
    raw          = google_bigquery_dataset.raw.dataset_id
    staging      = google_bigquery_dataset.staging.dataset_id
    intermediate = google_bigquery_dataset.intermediate.dataset_id
    marts        = google_bigquery_dataset.marts.dataset_id
  }
}

# ============================================
# Pub/Sub Outputs
# ============================================
output "pubsub_topics" {
  description = "Pub/Sub topic names"
  value = {
    listen_events        = google_pubsub_topic.listen_events.name
    page_view_events     = google_pubsub_topic.page_view_events.name
    auth_events          = google_pubsub_topic.auth_events.name
    status_change_events = google_pubsub_topic.status_change_events.name
  }
}

output "pubsub_subscriptions" {
  description = "Pub/Sub subscription names"
  value = {
    listen_events        = google_pubsub_subscription.listen_events_spark.name
    page_view_events     = google_pubsub_subscription.page_view_events_spark.name
    auth_events          = google_pubsub_subscription.auth_events_spark.name
    status_change_events = google_pubsub_subscription.status_change_events_spark.name
  }
}

# ============================================
# Service Account Outputs
# ============================================
output "service_accounts" {
  description = "Service account emails"
  value = {
    pipeline = google_service_account.pipeline_sa.email
    spark    = google_service_account.spark_sa.email
    dbt      = google_service_account.dbt_sa.email
    dagster  = google_service_account.dagster_sa.email
  }
}

output "service_account_key_paths" {
  description = "Paths to service account key files"
  value = {
    pipeline = local_file.pipeline_key.filename
    spark    = local_file.spark_key.filename
    dbt      = local_file.dbt_key.filename
    dagster  = local_file.dagster_key.filename
  }
  sensitive = true
}

# ============================================
# Connection Strings (for apps)
# ============================================
output "dbt_connection_config" {
  description = "dbt connection configuration"
  value = {
    project  = var.project_id
    dataset  = google_bigquery_dataset.staging.dataset_id
    location = var.bq_location
  }
}

output "spark_config" {
  description = "Spark streaming configuration"
  value = {
    gcs_checkpoint_path = "gs://${google_storage_bucket.data_lake.name}/checkpoints/spark"
    gcs_raw_path        = "gs://${google_storage_bucket.data_lake.name}/raw"
    pubsub_project      = var.project_id
  }
}

# ============================================
# Dataproc Outputs
# ============================================
output "dataproc_cluster" {
  description = "Dataproc cluster information"
  value = var.enable_dataproc ? {
    name           = google_dataproc_cluster.spark_streaming[0].name
    region         = var.region
    master_type    = var.dataproc_master_machine_type
    num_workers    = var.dataproc_num_workers
    worker_type    = var.dataproc_worker_machine_type
    web_ui         = "https://console.cloud.google.com/dataproc/clusters/${google_dataproc_cluster.spark_streaming[0].name}/monitoring?project=${var.project_id}&region=${var.region}"
    jupyter_url    = "https://${google_dataproc_cluster.spark_streaming[0].name}-m.${var.region}-a.c.${var.project_id}.internal:8123"
    spark_app_path = "gs://${google_storage_bucket.data_lake.name}/spark-apps/streaming_to_gcs_pubsub.py"
  } : null
}

output "dataproc_submit_command" {
  description = "Command to submit Spark streaming job"
  value = var.enable_dataproc ? "gcloud dataproc jobs submit pyspark gs://${google_storage_bucket.data_lake.name}/spark-apps/streaming_to_gcs_pubsub.py --project=${var.project_id} --region=${var.region} --cluster=soundflow-spark-${var.environment} --properties=\"spark.jars.packages=com.google.cloud.spark:spark-pubsub_2.12:0.21.0\"" : "Dataproc not enabled. Set enable_dataproc=true to create cluster."
}
