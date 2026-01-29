"""
SoundFlow Dagster Pipeline
===========================
Orchestrates dbt transformations for the music streaming data pipeline.

Supports:
- Local development (PostgreSQL)
- Production (BigQuery on GCP)

Pipeline Flow:
1. Check raw data availability
2. Run dbt transformations (staging → intermediate → marts)
3. Run dbt tests for data quality
4. Report status

Schedule: Configurable (5-minute incremental, hourly full refresh)
"""

import os
from pathlib import Path

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
    sensor,
    RunRequest,
    SensorEvaluationContext,
    DefaultSensorStatus,
)

# Import assets
from .assets.dbt_assets import (
    dbt_staging_models,
    dbt_intermediate_models,
    dbt_marts_models,
    dbt_test_results,
    dbt_docs,
    DbtConfig,
)

from .assets.bigquery_assets import (
    bq_raw_listen_events,
    bq_raw_page_view_events,
    bq_raw_auth_events,
    bq_raw_status_change_events,
    validate_mart_top_songs,
    validate_mart_active_users,
)

from .assets.spark_assets import (
    spark_job_status,
    gcs_data_freshness,
)

from .assets.etl_assets import (
    kafka_to_gcs_transfer,
    kafka_health_check,
    eventsim_generate,
)


# ============================================
# ALL ASSETS
# ============================================

all_dbt_assets = [
    dbt_staging_models,
    dbt_intermediate_models,
    dbt_marts_models,
    dbt_test_results,
    dbt_docs,
]

all_bigquery_assets = [
    bq_raw_listen_events,
    bq_raw_page_view_events,
    bq_raw_auth_events,
    bq_raw_status_change_events,
    validate_mart_top_songs,
    validate_mart_active_users,
]

all_spark_assets = [
    spark_job_status,
    gcs_data_freshness,
]

all_etl_assets = [
    kafka_health_check,
    kafka_to_gcs_transfer,
    eventsim_generate,
]


# ============================================
# JOBS
# ============================================

# Full dbt pipeline (staging → intermediate → marts → tests)
dbt_full_pipeline_job = define_asset_job(
    name="dbt_full_pipeline",
    selection=AssetSelection.assets(*all_dbt_assets),
    description="Run complete dbt transformation pipeline",
    config={
        "ops": {
            "dbt_staging_models": {"config": {"target": "prod"}},
            "dbt_intermediate_models": {"config": {"target": "prod"}},
            "dbt_marts_models": {"config": {"target": "prod"}},
            "dbt_test_results": {"config": {"target": "prod"}},
            "dbt_docs": {"config": {"target": "prod"}},
        }
    }
)

# Local dbt pipeline (for development)
dbt_local_pipeline_job = define_asset_job(
    name="dbt_local_pipeline",
    selection=AssetSelection.assets(*all_dbt_assets),
    description="Run dbt pipeline locally (PostgreSQL)",
    config={
        "ops": {
            "dbt_staging_models": {"config": {"target": "local"}},
            "dbt_intermediate_models": {"config": {"target": "local"}},
            "dbt_marts_models": {"config": {"target": "local"}},
            "dbt_test_results": {"config": {"target": "local"}},
            "dbt_docs": {"config": {"target": "local"}},
        }
    }
)

# Incremental marts only (for frequent updates)
dbt_marts_only_job = define_asset_job(
    name="dbt_marts_only",
    selection=AssetSelection.assets(dbt_marts_models, dbt_test_results),
    description="Run only marts (incremental) and tests",
    config={
        "ops": {
            "dbt_marts_models": {"config": {"target": "prod"}},
            "dbt_test_results": {"config": {"target": "prod"}},
        }
    }
)

# BigQuery validation job
bigquery_validation_job = define_asset_job(
    name="bigquery_validation",
    selection=AssetSelection.assets(*all_bigquery_assets),
    description="Validate BigQuery raw data and marts"
)

# Infrastructure check job
infrastructure_check_job = define_asset_job(
    name="infrastructure_check",
    selection=AssetSelection.assets(*all_spark_assets),
    description="Check Spark/GCS infrastructure status"
)

# ETL job - Kafka to GCS transfer
etl_kafka_to_gcs_job = define_asset_job(
    name="etl_kafka_to_gcs",
    selection=AssetSelection.assets(kafka_health_check, kafka_to_gcs_transfer),
    description="Transfer data from Kafka to GCS"
)

# Full pipeline job - EventSim → Kafka → GCS → dbt
full_pipeline_job = define_asset_job(
    name="full_pipeline",
    selection=AssetSelection.assets(
        kafka_health_check,
        eventsim_generate,
        kafka_to_gcs_transfer,
        gcs_data_freshness,
        dbt_staging_models,
        dbt_intermediate_models,
        dbt_marts_models,
        dbt_test_results,
    ),
    description="Run complete pipeline: EventSim → Kafka → GCS → dbt",
    config={
        "ops": {
            "dbt_staging_models": {"config": {"target": "prod"}},
            "dbt_intermediate_models": {"config": {"target": "prod"}},
            "dbt_marts_models": {"config": {"target": "prod"}},
            "dbt_test_results": {"config": {"target": "prod"}},
        }
    }
)


# ============================================
# SCHEDULES
# ============================================

# Every 15 minutes - incremental marts update
frequent_marts_schedule = ScheduleDefinition(
    job=dbt_marts_only_job,
    cron_schedule="*/15 * * * *",
    name="frequent_marts_update",
    description="Update marts every 15 minutes (incremental)"
)

# Hourly - full dbt pipeline
hourly_full_pipeline_schedule = ScheduleDefinition(
    job=dbt_full_pipeline_job,
    cron_schedule="0 * * * *",
    name="hourly_full_pipeline",
    description="Run full dbt pipeline every hour at minute 0"
)

# Daily at 2 AM - full pipeline with docs
daily_full_pipeline_schedule = ScheduleDefinition(
    job=dbt_full_pipeline_job,
    cron_schedule="0 2 * * *",
    name="daily_full_pipeline",
    description="Run full dbt pipeline with docs at 2 AM daily"
)

# Every 30 minutes - BigQuery validation
bigquery_validation_schedule = ScheduleDefinition(
    job=bigquery_validation_job,
    cron_schedule="*/30 * * * *",
    name="bigquery_validation_check",
    description="Validate BigQuery data every 30 minutes"
)

# Hourly - ETL Kafka to GCS
hourly_etl_schedule = ScheduleDefinition(
    job=etl_kafka_to_gcs_job,
    cron_schedule="30 * * * *",
    name="hourly_etl_transfer",
    description="Transfer new Kafka data to GCS every hour at minute 30"
)

# Daily at 1 AM - Full pipeline
daily_full_pipeline_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 1 * * *",
    name="daily_full_pipeline_run",
    description="Run complete pipeline daily at 1 AM"
)


# ============================================
# SENSORS
# ============================================

@sensor(
    job=dbt_full_pipeline_job,
    minimum_interval_seconds=300,  # Check every 5 minutes
    default_status=DefaultSensorStatus.STOPPED,
)
def gcs_new_data_sensor(context: SensorEvaluationContext):
    """
    Sensor that triggers dbt pipeline when new data arrives in GCS.
    
    Monitors GCS for new Parquet files and triggers transformation
    when fresh data is detected.
    """
    from google.cloud import storage
    from datetime import datetime, timedelta
    
    try:
        project_id = os.getenv("GCP_PROJECT", "graphic-boulder-483814-g7")
        bucket_name = os.getenv("GCS_BUCKET", "tf-state-soundflow-123")
        
        credentials_path = os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS",
            str(Path(__file__).parent.parent.parent / "credentials" / "dbt-sa-key.json")
        )
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        
        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        
        # Check for files modified in last 10 minutes
        cutoff = datetime.utcnow() - timedelta(minutes=10)
        
        # Look for listen_events (primary data source)
        blobs = list(bucket.list_blobs(prefix="raw/listen_events/", max_results=50))
        
        recent_files = [b for b in blobs if b.updated.replace(tzinfo=None) > cutoff]
        
        if recent_files:
            latest = max(recent_files, key=lambda b: b.updated)
            run_key = f"gcs_{latest.updated.strftime('%Y%m%d_%H%M%S')}"
            
            context.log.info(f"New data detected: {latest.name}")
            yield RunRequest(run_key=run_key)
        else:
            context.log.info("No new data in GCS")
            
    except Exception as e:
        context.log.error(f"Sensor error: {e}")


# ============================================
# DEFINITIONS (Main entry point)
# ============================================

defs = Definitions(
    assets=[
        # dbt assets
        dbt_staging_models,
        dbt_intermediate_models,
        dbt_marts_models,
        dbt_test_results,
        dbt_docs,
        # BigQuery validation assets
        bq_raw_listen_events,
        bq_raw_page_view_events,
        bq_raw_auth_events,
        bq_raw_status_change_events,
        validate_mart_top_songs,
        validate_mart_active_users,
        # Spark/Infrastructure assets
        spark_job_status,
        gcs_data_freshness,
        # ETL assets
        kafka_health_check,
        kafka_to_gcs_transfer,
        eventsim_generate,
    ],
    jobs=[
        dbt_full_pipeline_job,
        dbt_local_pipeline_job,
        dbt_marts_only_job,
        bigquery_validation_job,
        infrastructure_check_job,
        etl_kafka_to_gcs_job,
        full_pipeline_job,
    ],
    schedules=[
        frequent_marts_schedule,
        hourly_full_pipeline_schedule,
        daily_full_pipeline_schedule,
        bigquery_validation_schedule,
        hourly_etl_schedule,
        daily_full_pipeline_schedule,
    ],
    sensors=[
        gcs_new_data_sensor,
    ],
)
