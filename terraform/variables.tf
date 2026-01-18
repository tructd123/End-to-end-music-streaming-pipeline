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

variable "enable_external_tables" {
  description = "Enable BigQuery external tables. Set to true after Spark streaming has written data to GCS."
  type        = bool
  default     = false
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

# ============================================
# DATAPROC VARIABLES
# ============================================

variable "enable_dataproc" {
  description = "Enable Dataproc cluster for Spark Streaming"
  type        = bool
  default     = false
}

variable "dataproc_master_machine_type" {
  description = "Machine type for Dataproc master node"
  type        = string
  default     = "n1-standard-4"  # 4 vCPU, 15GB RAM
}

variable "dataproc_master_disk_size" {
  description = "Boot disk size for Dataproc master node (GB)"
  type        = number
  default     = 100
}

variable "dataproc_worker_machine_type" {
  description = "Machine type for Dataproc worker nodes"
  type        = string
  default     = "n1-standard-4"  # 4 vCPU, 15GB RAM
}

variable "dataproc_worker_disk_size" {
  description = "Boot disk size for Dataproc worker nodes (GB)"
  type        = number
  default     = 100
}

variable "dataproc_num_workers" {
  description = "Number of Dataproc worker nodes"
  type        = number
  default     = 2
}

variable "dataproc_num_preemptible_workers" {
  description = "Number of preemptible worker nodes (cost optimization)"
  type        = number
  default     = 0
}

variable "dataproc_subnetwork" {
  description = "Subnetwork for Dataproc cluster (optional)"
  type        = string
  default     = ""
}

variable "dataproc_internal_ip_only" {
  description = "Use internal IP only for Dataproc cluster"
  type        = bool
  default     = false
}

variable "enable_dataproc_autoscaling" {
  description = "Enable autoscaling for Dataproc cluster"
  type        = bool
  default     = false
}

variable "enable_workflow_template" {
  description = "Enable Dataproc workflow template for batch processing"
  type        = bool
  default     = false
}
