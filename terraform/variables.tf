# Terraform Variables for SoundFlow Music Streaming Pipeline

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region for compute resources"
  type        = string
  default     = "asia-southeast1"
}

variable "bq_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "asia-southeast1"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "gcs_bucket_name" {
  description = "Name of the GCS bucket for data lake"
  type        = string
}

variable "enable_cloud_run" {
  description = "Enable Cloud Run services for Spark and Dagster"
  type        = bool
  default     = false
}

variable "dagster_image" {
  description = "Docker image for Dagster Cloud Run service"
  type        = string
  default     = ""
}

variable "spark_image" {
  description = "Docker image for Spark Streaming Cloud Run service"
  type        = string
  default     = ""
}
