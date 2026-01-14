# ============================================
# SERVICE ACCOUNTS & IAM
# ============================================

# Main pipeline service account
resource "google_service_account" "pipeline_sa" {
  account_id   = "soundflow-pipeline"
  display_name = "SoundFlow Pipeline Service Account"
  description  = "Service account for Spark, dbt, and Dagster access"
  
  depends_on = [google_project_service.required_apis]
}

# Spark Streaming service account
resource "google_service_account" "spark_sa" {
  account_id   = "soundflow-spark"
  display_name = "SoundFlow Spark Streaming"
  description  = "Service account for Spark Streaming jobs"
  
  depends_on = [google_project_service.required_apis]
}

# dbt service account
resource "google_service_account" "dbt_sa" {
  account_id   = "soundflow-dbt"
  display_name = "SoundFlow dbt"
  description  = "Service account for dbt transformations"
  
  depends_on = [google_project_service.required_apis]
}

# Dagster service account
resource "google_service_account" "dagster_sa" {
  account_id   = "soundflow-dagster"
  display_name = "SoundFlow Dagster"
  description  = "Service account for Dagster orchestration"
  
  depends_on = [google_project_service.required_apis]
}

# ============================================
# IAM BINDINGS - Pipeline SA (main)
# ============================================

resource "google_storage_bucket_iam_member" "pipeline_storage_admin" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "pipeline_bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# ============================================
# IAM BINDINGS - Spark SA
# ============================================

# GCS access for reading/writing data
resource "google_storage_bucket_iam_member" "spark_storage_writer" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.spark_sa.email}"
}

# Pub/Sub subscriber for consuming messages
resource "google_project_iam_member" "spark_pubsub_subscriber" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.spark_sa.email}"
}

# BigQuery for writing to tables (optional direct write)
resource "google_project_iam_member" "spark_bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.spark_sa.email}"
}

resource "google_project_iam_member" "spark_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.spark_sa.email}"
}

# ============================================
# IAM BINDINGS - dbt SA
# ============================================

# BigQuery admin for creating/modifying tables
resource "google_project_iam_member" "dbt_bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dbt_sa.email}"
}

resource "google_project_iam_member" "dbt_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dbt_sa.email}"
}

# GCS read access for external tables
resource "google_storage_bucket_iam_member" "dbt_storage_viewer" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.dbt_sa.email}"
}

# ============================================
# IAM BINDINGS - Dagster SA
# ============================================

# BigQuery access
resource "google_project_iam_member" "dagster_bq_data_viewer" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.dagster_sa.email}"
}

resource "google_project_iam_member" "dagster_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dagster_sa.email}"
}

# Cloud Run invoker (to trigger jobs)
resource "google_project_iam_member" "dagster_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.dagster_sa.email}"
}

# Pub/Sub publisher (for triggering)
resource "google_project_iam_member" "dagster_pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.dagster_sa.email}"
}

# Service Account Token Creator (for impersonation)
resource "google_service_account_iam_member" "dagster_token_creator_dbt" {
  service_account_id = google_service_account.dbt_sa.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${google_service_account.dagster_sa.email}"
}

# ============================================
# SERVICE ACCOUNT KEYS
# ============================================

resource "google_service_account_key" "pipeline_sa_key" {
  service_account_id = google_service_account.pipeline_sa.name
}

resource "google_service_account_key" "spark_sa_key" {
  service_account_id = google_service_account.spark_sa.name
}

resource "google_service_account_key" "dbt_sa_key" {
  service_account_id = google_service_account.dbt_sa.name
}

resource "google_service_account_key" "dagster_sa_key" {
  service_account_id = google_service_account.dagster_sa.name
}

# Save keys to local files
resource "local_file" "pipeline_key" {
  content         = base64decode(google_service_account_key.pipeline_sa_key.private_key)
  filename        = "${path.module}/../credentials/pipeline-sa-key.json"
  file_permission = "0600"
}

resource "local_file" "spark_key" {
  content         = base64decode(google_service_account_key.spark_sa_key.private_key)
  filename        = "${path.module}/../credentials/spark-sa-key.json"
  file_permission = "0600"
}

resource "local_file" "dbt_key" {
  content         = base64decode(google_service_account_key.dbt_sa_key.private_key)
  filename        = "${path.module}/../credentials/dbt-sa-key.json"
  file_permission = "0600"
}

resource "local_file" "dagster_key" {
  content         = base64decode(google_service_account_key.dagster_sa_key.private_key)
  filename        = "${path.module}/../credentials/dagster-sa-key.json"
  file_permission = "0600"
}
