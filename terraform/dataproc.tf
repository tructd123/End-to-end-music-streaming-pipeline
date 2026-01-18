# ============================================
# DATAPROC - Managed Spark Cluster
# ============================================

# Dataproc cluster for Spark Streaming
resource "google_dataproc_cluster" "spark_streaming" {
  count  = var.enable_dataproc ? 1 : 0
  name   = "soundflow-spark-${var.environment}"
  region = var.region

  labels = {
    environment = var.environment
    project     = "soundflow"
    managed_by  = "terraform"
  }

  cluster_config {
    # Staging bucket for cluster data
    staging_bucket = google_storage_bucket.data_lake.name

    # Master node configuration
    master_config {
      num_instances = 1
      machine_type  = var.dataproc_master_machine_type

      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = var.dataproc_master_disk_size
      }
    }

    # Worker nodes configuration
    worker_config {
      num_instances = var.dataproc_num_workers
      machine_type  = var.dataproc_worker_machine_type

      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = var.dataproc_worker_disk_size
        num_local_ssds    = 0
      }
    }

    # Preemptible workers (cost optimization)
    preemptible_worker_config {
      num_instances = var.dataproc_num_preemptible_workers
    }

    # Software configuration
    software_config {
      image_version = "2.1-debian11"  # Spark 3.3, Hadoop 3.3

      override_properties = {
        # Spark configurations
        "spark:spark.sql.adaptive.enabled"                    = "true"
        "spark:spark.sql.adaptive.coalescePartitions.enabled" = "true"
        "spark:spark.dynamicAllocation.enabled"               = "true"
        "spark:spark.dynamicAllocation.minExecutors"          = "1"
        "spark:spark.dynamicAllocation.maxExecutors"          = "10"
        "spark:spark.streaming.stopGracefullyOnShutdown"      = "true"
        
        # GCS connector
        "spark:spark.hadoop.fs.gs.impl"                       = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        "spark:spark.hadoop.google.cloud.auth.service.account.enable" = "true"
        
        # Parquet optimization
        "spark:spark.sql.parquet.compression.codec"           = "snappy"
        "spark:spark.sql.parquet.mergeSchema"                 = "false"
      }

      # Optional components
      optional_components = [
        "JUPYTER",      # For interactive development
        "ZEPPELIN",     # Alternative notebook
      ]
    }

    # GCE cluster configuration
    gce_cluster_config {
      zone = "${var.region}-b"  # Using zone b as zone a may have resource constraints

      # Service account for cluster
      service_account        = google_service_account.spark_sa.email
      service_account_scopes = ["cloud-platform"]

      # Network configuration
      subnetwork = var.dataproc_subnetwork != "" ? var.dataproc_subnetwork : null

      # Internal IP only (more secure)
      internal_ip_only = var.dataproc_internal_ip_only

      # Metadata
      metadata = {
        "enable-oslogin" = "true"
      }

      # Shielded instance config
      shielded_instance_config {
        enable_secure_boot          = true
        enable_vtpm                 = true
        enable_integrity_monitoring = true
      }
    }

    # Initialization actions (install dependencies)
    initialization_action {
      script      = "gs://${google_storage_bucket.data_lake.name}/scripts/dataproc-init.sh"
      timeout_sec = 300
    }

    # Autoscaling policy (optional)
    dynamic "autoscaling_config" {
      for_each = var.enable_dataproc_autoscaling ? [1] : []
      content {
        policy_uri = google_dataproc_autoscaling_policy.spark_autoscaling[0].name
      }
    }

    # Endpoint config
    endpoint_config {
      enable_http_port_access = true
    }
  }

  depends_on = [
    google_project_service.required_apis,
    google_storage_bucket_object.dataproc_init_script
  ]
}

# Autoscaling policy
resource "google_dataproc_autoscaling_policy" "spark_autoscaling" {
  count     = var.enable_dataproc && var.enable_dataproc_autoscaling ? 1 : 0
  policy_id = "soundflow-autoscaling-${var.environment}"
  location  = var.region

  worker_config {
    min_instances = 2
    max_instances = 10
    weight        = 1
  }

  secondary_worker_config {
    min_instances = 0
    max_instances = 20
    weight        = 1
  }

  basic_algorithm {
    yarn_config {
      graceful_decommission_timeout = "60s"
      scale_up_factor               = 1.0
      scale_down_factor             = 1.0
      scale_up_min_worker_fraction  = 0.0
      scale_down_min_worker_fraction = 0.0
    }
  }
}

# ============================================
# DATAPROC INITIALIZATION SCRIPT
# ============================================

resource "google_storage_bucket_object" "dataproc_init_script" {
  count        = var.enable_dataproc ? 1 : 0
  name         = "scripts/dataproc-init.sh"
  bucket       = google_storage_bucket.data_lake.name
  source       = "${path.module}/scripts/dataproc-init.sh"
  content_type = "text/x-shellscript"
}

# ============================================
# DATAPROC JOBS - Spark Streaming
# ============================================

# Upload Spark streaming application to GCS
resource "google_storage_bucket_object" "spark_streaming_app" {
  count  = var.enable_dataproc ? 1 : 0
  name   = "spark-apps/streaming_to_gcs_pubsub.py"
  bucket = google_storage_bucket.data_lake.name
  source = "${path.module}/../spark_streaming/src/streaming_to_gcs_pubsub.py"
}

# Dataproc job for Spark Streaming (can be submitted via gcloud or Dagster)
# This is a reference template - actual job submission is done via CLI or API
resource "local_file" "spark_job_template" {
  count    = var.enable_dataproc ? 1 : 0
  filename = "${path.module}/../scripts/submit_spark_job.sh"
  
  content = <<-EOF
#!/bin/bash
# Submit Spark Streaming job to Dataproc
# Usage: ./submit_spark_job.sh

set -e

PROJECT_ID="${var.project_id}"
REGION="${var.region}"
CLUSTER_NAME="soundflow-spark-${var.environment}"
GCS_BUCKET="${google_storage_bucket.data_lake.name}"

echo "Submitting Spark Streaming job to Dataproc..."

gcloud dataproc jobs submit pyspark \
    gs://$GCS_BUCKET/spark-apps/streaming_to_gcs_pubsub.py \
    --project=$PROJECT_ID \
    --region=$REGION \
    --cluster=$CLUSTER_NAME \
    --properties="\
spark.sql.adaptive.enabled=true,\
spark.streaming.stopGracefullyOnShutdown=true,\
spark.jars.packages=com.google.cloud.spark:spark-pubsub_2.12:0.21.0" \
    -- \
    --source-type=pubsub \
    --gcp-project=$PROJECT_ID \
    --gcs-bucket=$GCS_BUCKET \
    --trigger-interval="2 minutes"

echo "Job submitted successfully!"
EOF

  file_permission = "0755"
}

# ============================================
# WORKFLOW TEMPLATE (Optional - for scheduled jobs)
# ============================================

resource "google_dataproc_workflow_template" "spark_streaming_workflow" {
  count    = var.enable_dataproc && var.enable_workflow_template ? 1 : 0
  name     = "soundflow-streaming-workflow"
  location = var.region

  placement {
    managed_cluster {
      cluster_name = "soundflow-ephemeral-${var.environment}"

      config {
        gce_cluster_config {
          zone = "${var.region}-a"
          service_account        = google_service_account.spark_sa.email
          service_account_scopes = ["cloud-platform"]
        }

        master_config {
          num_instances = 1
          machine_type  = "n1-standard-4"
          disk_config {
            boot_disk_type    = "pd-ssd"
            boot_disk_size_gb = 100
          }
        }

        worker_config {
          num_instances = 2
          machine_type  = "n1-standard-4"
          disk_config {
            boot_disk_type    = "pd-ssd"
            boot_disk_size_gb = 100
          }
        }

        software_config {
          image_version = "2.1-debian11"
        }
      }
    }
  }

  jobs {
    step_id = "spark-streaming"

    pyspark_job {
      main_python_file_uri = "gs://${google_storage_bucket.data_lake.name}/spark-apps/streaming_to_gcs_pubsub.py"
      
      properties = {
        "spark.sql.adaptive.enabled"               = "true"
        "spark.streaming.stopGracefullyOnShutdown" = "true"
      }

      args = [
        "--source-type=pubsub",
        "--gcp-project=${var.project_id}",
        "--gcs-bucket=${google_storage_bucket.data_lake.name}",
      ]
    }
  }

  labels = {
    environment = var.environment
  }
}
