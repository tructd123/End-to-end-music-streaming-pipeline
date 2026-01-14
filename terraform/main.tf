# Terraform Configuration for Music Streaming Data Pipeline
# Creates GCS bucket, BigQuery dataset, Pub/Sub, Cloud Run on GCP

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
  #   prefix = "soundflow-pipeline"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "pubsub.googleapis.com",
    "run.googleapis.com",
    "containerregistry.googleapis.com",
    "artifactregistry.googleapis.com",
    "dataflow.googleapis.com",
    "compute.googleapis.com",
  ])
  
  project = var.project_id
  service = each.value
  
  disable_on_destroy = false
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
    project     = "soundflow"
    managed_by  = "terraform"
  }
  
  depends_on = [google_project_service.required_apis]
}

# Create folder structure in GCS
resource "google_storage_bucket_object" "folders" {
  for_each = toset([
    "raw/listen_events/",
    "raw/page_view_events/",
    "raw/auth_events/",
    "raw/status_change_events/",
    "processed/",
    "checkpoints/spark/",
    "checkpoints/dagster/",
    "dbt/logs/",
    "dbt/target/",
  ])
  
  name    = each.value
  content = " "
  bucket  = google_storage_bucket.data_lake.name
}

# Note: BigQuery datasets are defined in bigquery.tf
# Note: Service accounts and IAM are defined in iam.tf
# Note: Pub/Sub topics are defined in pubsub.tf
