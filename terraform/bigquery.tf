# ============================================
# BIGQUERY DATASETS - Data Warehouse Layers
# ============================================

# Raw dataset - for external tables from GCS
resource "google_bigquery_dataset" "raw" {
  dataset_id    = "raw"
  friendly_name = "Raw Data Layer"
  description   = "External tables pointing to GCS raw data (Parquet)"
  location      = var.bq_location
  
  default_table_expiration_ms = null
  
  labels = {
    environment = var.environment
    layer       = "raw"
    managed_by  = "terraform"
  }
  
  access {
    role          = "OWNER"
    user_by_email = google_service_account.pipeline_sa.email
  }
  
  depends_on = [google_project_service.required_apis]
}

# Staging dataset - for dbt staging models
resource "google_bigquery_dataset" "staging" {
  dataset_id    = "staging"
  friendly_name = "Staging Layer"
  description   = "Cleaned and standardized data from dbt staging models"
  location      = var.bq_location
  
  default_table_expiration_ms = null
  
  labels = {
    environment = var.environment
    layer       = "staging"
    managed_by  = "terraform"
  }
  
  access {
    role          = "OWNER"
    user_by_email = google_service_account.pipeline_sa.email
  }
  
  depends_on = [google_project_service.required_apis]
}

# Intermediate dataset - for dbt intermediate models
resource "google_bigquery_dataset" "intermediate" {
  dataset_id    = "intermediate"
  friendly_name = "Intermediate Layer"
  description   = "Aggregated data from dbt intermediate models"
  location      = var.bq_location
  
  default_table_expiration_ms = null
  
  labels = {
    environment = var.environment
    layer       = "intermediate"
    managed_by  = "terraform"
  }
  
  access {
    role          = "OWNER"
    user_by_email = google_service_account.pipeline_sa.email
  }
  
  depends_on = [google_project_service.required_apis]
}

# Marts dataset - for dbt marts models (analytics-ready)
resource "google_bigquery_dataset" "marts" {
  dataset_id    = "marts"
  friendly_name = "Data Marts"
  description   = "Analytics-ready tables for BI and reporting"
  location      = var.bq_location
  
  default_table_expiration_ms = null
  
  labels = {
    environment = var.environment
    layer       = "marts"
    managed_by  = "terraform"
  }
  
  access {
    role          = "OWNER"
    user_by_email = google_service_account.pipeline_sa.email
  }
  
  # Allow BI tools to read
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
  
  depends_on = [google_project_service.required_apis]
}

# ============================================
# EXTERNAL TABLES - Raw data from GCS
# ============================================
# NOTE: External tables are disabled by default because they require
# existing parquet files in GCS. Enable them after Spark streaming
# has written data to GCS by setting enable_external_tables = true.
# ============================================

resource "google_bigquery_table" "external_listen_events" {
  count               = var.enable_external_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "listen_events"
  deletion_protection = false
  
  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.data_lake.name}/raw/listen_events/*.parquet"]
    
    hive_partitioning_options {
      mode                     = "AUTO"
      source_uri_prefix        = "gs://${google_storage_bucket.data_lake.name}/raw/listen_events"
      require_partition_filter = false
    }
  }
  
  labels = {
    environment = var.environment
    event_type  = "listen"
  }
}

resource "google_bigquery_table" "external_page_view_events" {
  count               = var.enable_external_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "page_view_events"
  deletion_protection = false
  
  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.data_lake.name}/raw/page_view_events/*.parquet"]
    
    hive_partitioning_options {
      mode                     = "AUTO"
      source_uri_prefix        = "gs://${google_storage_bucket.data_lake.name}/raw/page_view_events"
      require_partition_filter = false
    }
  }
  
  labels = {
    environment = var.environment
    event_type  = "page_view"
  }
}

resource "google_bigquery_table" "external_auth_events" {
  count               = var.enable_external_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "auth_events"
  deletion_protection = false
  
  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.data_lake.name}/raw/auth_events/*.parquet"]
    
    hive_partitioning_options {
      mode                     = "AUTO"
      source_uri_prefix        = "gs://${google_storage_bucket.data_lake.name}/raw/auth_events"
      require_partition_filter = false
    }
  }
  
  labels = {
    environment = var.environment
    event_type  = "auth"
  }
}

resource "google_bigquery_table" "external_status_change_events" {
  count               = var.enable_external_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "status_change_events"
  deletion_protection = false
  
  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.data_lake.name}/raw/status_change_events/*.parquet"]
    
    hive_partitioning_options {
      mode                     = "AUTO"
      source_uri_prefix        = "gs://${google_storage_bucket.data_lake.name}/raw/status_change_events"
      require_partition_filter = false
    }
  }
  
  labels = {
    environment = var.environment
    event_type  = "status_change"
  }
}
