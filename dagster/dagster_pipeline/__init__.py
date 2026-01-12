"""
Dagster Pipeline for Music Streaming Analytics
Orchestrates hourly batch jobs: GCS -> BigQuery -> dbt transformations
"""

from dagster import (
    Definitions,
    asset,
    AssetExecutionContext,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
    Config,
    EnvVar
)
from dagster_gcp import BigQueryResource, GCSResource
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject
import os
from pathlib import Path

# Configuration
GCS_BUCKET = os.getenv("GCS_BUCKET", "music-streaming-data-lake")
GCP_PROJECT = os.getenv("GCP_PROJECT", "your-project-id")
BQ_DATASET = os.getenv("BQ_DATASET", "music_streaming")

# dbt project path
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt"


# ============================================
# ASSETS - Data Pipeline Components
# ============================================

@asset(
    group_name="ingestion",
    description="Load listen events from GCS to BigQuery staging table"
)
def stg_listen_events(
    context: AssetExecutionContext,
    bigquery: BigQueryResource
) -> None:
    """Load listen events from GCS external table to staging"""
    
    query = f"""
    CREATE OR REPLACE TABLE `{GCP_PROJECT}.{BQ_DATASET}.stg_listen_events` AS
    SELECT 
        ts,
        event_timestamp,
        userId as user_id,
        sessionId as session_id,
        song,
        artist,
        duration,
        level,
        firstName as first_name,
        lastName as last_name,
        gender,
        location,
        lat as latitude,
        lng as longitude,
        userAgent as user_agent,
        processed_at,
        year,
        month,
        day,
        hour
    FROM `{GCP_PROJECT}.{BQ_DATASET}.external_listen_events`
    WHERE processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
    """
    
    with bigquery.get_client() as client:
        job = client.query(query)
        job.result()
        context.log.info(f"Loaded {job.num_dml_affected_rows} rows to stg_listen_events")


@asset(
    group_name="ingestion",
    description="Load page view events from GCS to BigQuery staging table"
)
def stg_page_view_events(
    context: AssetExecutionContext,
    bigquery: BigQueryResource
) -> None:
    """Load page view events from GCS external table to staging"""
    
    query = f"""
    CREATE OR REPLACE TABLE `{GCP_PROJECT}.{BQ_DATASET}.stg_page_view_events` AS
    SELECT 
        ts,
        event_timestamp,
        userId as user_id,
        sessionId as session_id,
        page,
        method,
        status,
        level,
        auth,
        firstName as first_name,
        lastName as last_name,
        gender,
        location,
        lat as latitude,
        lng as longitude,
        userAgent as user_agent,
        processed_at,
        year,
        month,
        day,
        hour
    FROM `{GCP_PROJECT}.{BQ_DATASET}.external_page_view_events`
    WHERE processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
    """
    
    with bigquery.get_client() as client:
        job = client.query(query)
        job.result()
        context.log.info(f"Loaded {job.num_dml_affected_rows} rows to stg_page_view_events")


@asset(
    group_name="ingestion", 
    description="Load auth events from GCS to BigQuery staging table"
)
def stg_auth_events(
    context: AssetExecutionContext,
    bigquery: BigQueryResource
) -> None:
    """Load auth events from GCS external table to staging"""
    
    query = f"""
    CREATE OR REPLACE TABLE `{GCP_PROJECT}.{BQ_DATASET}.stg_auth_events` AS
    SELECT 
        ts,
        event_timestamp,
        userId as user_id,
        sessionId as session_id,
        page,
        auth,
        level,
        firstName as first_name,
        lastName as last_name,
        gender,
        location,
        userAgent as user_agent,
        success,
        processed_at,
        year,
        month,
        day,
        hour
    FROM `{GCP_PROJECT}.{BQ_DATASET}.external_auth_events`
    WHERE processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
    """
    
    with bigquery.get_client() as client:
        job = client.query(query)
        job.result()
        context.log.info(f"Loaded {job.num_dml_affected_rows} rows to stg_auth_events")


# ============================================
# DBT ASSETS - Transformations
# ============================================

# dbt project configuration
dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
)

@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Run dbt models for data transformation"""
    yield from dbt.cli(["build"], context=context).stream()


# ============================================
# JOBS & SCHEDULES
# ============================================

# Job to run all ingestion assets
ingestion_job = define_asset_job(
    name="hourly_ingestion_job",
    selection=AssetSelection.groups("ingestion"),
    description="Load data from GCS to BigQuery staging tables"
)

# Job to run dbt transformations
transformation_job = define_asset_job(
    name="hourly_transformation_job",
    selection=AssetSelection.all() - AssetSelection.groups("ingestion"),
    description="Run dbt transformations"
)

# Full pipeline job
full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.all(),
    description="Run full data pipeline: ingestion + transformation"
)

# Hourly schedule
hourly_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 * * * *",  # Every hour
    execution_timezone="Asia/Ho_Chi_Minh",
)


# ============================================
# RESOURCES
# ============================================

resources = {
    "bigquery": BigQueryResource(
        project=EnvVar("GCP_PROJECT"),
        location="asia-southeast1",
    ),
    "gcs": GCSResource(
        project=EnvVar("GCP_PROJECT"),
    ),
    "dbt": DbtCliResource(
        project_dir=dbt_project,
    ),
}


# ============================================
# DEFINITIONS
# ============================================

defs = Definitions(
    assets=[
        stg_listen_events,
        stg_page_view_events, 
        stg_auth_events,
        dbt_models,
    ],
    jobs=[
        ingestion_job,
        transformation_job,
        full_pipeline_job,
    ],
    schedules=[hourly_schedule],
    resources=resources,
)
