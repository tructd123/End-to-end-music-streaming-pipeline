# Terraform Configuration for Music Streaming Data Pipeline
# Creates GCS bucket, BigQuery dataset, and Service Account on GCP

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  
  # Optional: Configure remote backend for state storage
  # backend "gcs" {
  #   bucket = "your-terraform-state-bucket"
  #   prefix = "music-streaming-pipeline"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ============================================
# GOOGLE CLOUD STORAGE - Data Lake
# ============================================
resource "google_storage_bucket" "data_lake" {
  name          = var.gcs_bucket_name
  location      = var.region
  force_destroy = var.environment == "dev" ? true : false
  
  # Enable versioning for data protection
  versioning {
    enabled = true
  }
  
  # Lifecycle rules to manage storage costs
  lifecycle_rule {
    condition {
      age = 30  # Move to Nearline after 30 days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  
  lifecycle_rule {
    condition {
      age = 90  # Move to Coldline after 90 days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
  
  # Uniform bucket-level access
  uniform_bucket_level_access = true
  
  labels = {
    environment = var.environment
    project     = "music-streaming-pipeline"
    managed_by  = "terraform"
  }
}

# Create folder structure in GCS
resource "google_storage_bucket_object" "raw_folder" {
  name    = "raw/"
  content = " "
  bucket  = google_storage_bucket.data_lake.name
}

resource "google_storage_bucket_object" "processed_folder" {
  name    = "processed/"
  content = " "
  bucket  = google_storage_bucket.data_lake.name
}

resource "google_storage_bucket_object" "checkpoints_folder" {
  name    = "checkpoints/"
  content = " "
  bucket  = google_storage_bucket.data_lake.name
}

# ============================================
# BIGQUERY - Data Warehouse
# ============================================
resource "google_bigquery_dataset" "music_streaming" {
  dataset_id    = var.bq_dataset_id
  friendly_name = "Music Streaming Analytics"
  description   = "Dataset for music streaming event analytics"
  location      = var.region
  
  # Default table expiration (optional, set to 0 to disable)
  default_table_expiration_ms = null
  
  labels = {
    environment = var.environment
    project     = "music-streaming-pipeline"
    managed_by  = "terraform"
  }
  
  access {
    role          = "OWNER"
    user_by_email = google_service_account.pipeline_sa.email
  }
  
  access {
    role          = "WRITER"
    user_by_email = google_service_account.pipeline_sa.email
  }
}

# External tables pointing to GCS raw data
resource "google_bigquery_table" "external_listen_events" {
  dataset_id          = google_bigquery_dataset.music_streaming.dataset_id
  table_id            = "external_listen_events"
  deletion_protection = false
  
  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.data_lake.name}/raw/listen_events/*"]
    
    hive_partitioning_options {
      mode                     = "AUTO"
      source_uri_prefix        = "gs://${google_storage_bucket.data_lake.name}/raw/listen_events"
      require_partition_filter = false
    }
  }
  
  labels = {
    environment = var.environment
    layer       = "raw"
  }
}

resource "google_bigquery_table" "external_page_view_events" {
  dataset_id          = google_bigquery_dataset.music_streaming.dataset_id
  table_id            = "external_page_view_events"
  deletion_protection = false
  
  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.data_lake.name}/raw/page_view_events/*"]
    
    hive_partitioning_options {
      mode                     = "AUTO"
      source_uri_prefix        = "gs://${google_storage_bucket.data_lake.name}/raw/page_view_events"
      require_partition_filter = false
    }
  }
  
  labels = {
    environment = var.environment
    layer       = "raw"
  }
}

resource "google_bigquery_table" "external_auth_events" {
  dataset_id          = google_bigquery_dataset.music_streaming.dataset_id
  table_id            = "external_auth_events"
  deletion_protection = false
  
  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.data_lake.name}/raw/auth_events/*"]
    
    hive_partitioning_options {
      mode                     = "AUTO"
      source_uri_prefix        = "gs://${google_storage_bucket.data_lake.name}/raw/auth_events"
      require_partition_filter = false
    }
  }
  
  labels = {
    environment = var.environment
    layer       = "raw"
  }
}

# ============================================
# SERVICE ACCOUNT - Pipeline Access
# ============================================
resource "google_service_account" "pipeline_sa" {
  account_id   = "music-streaming-pipeline"
  display_name = "Music Streaming Pipeline Service Account"
  description  = "Service account for Spark, Dagster, and dbt access"
}

# Grant Storage Admin role to Service Account
resource "google_storage_bucket_iam_member" "pipeline_storage_admin" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Grant BigQuery Data Editor role
resource "google_project_iam_member" "pipeline_bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Grant BigQuery Job User role
resource "google_project_iam_member" "pipeline_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Create and download Service Account key
resource "google_service_account_key" "pipeline_sa_key" {
  service_account_id = google_service_account.pipeline_sa.name
}

# Save the key to local file
resource "local_file" "sa_key_file" {
  content  = base64decode(google_service_account_key.pipeline_sa_key.private_key)
  filename = "${path.module}/../credentials/gcp-key.json"
  
  file_permission = "0600"
}
