"""
BigQuery Assets for SoundFlow Pipeline
=======================================
Assets for checking BigQuery data availability on GCP.
"""

import os
from pathlib import Path

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    Output,
)


def get_bigquery_client():
    """Get BigQuery client"""
    from google.cloud import bigquery
    
    credentials_path = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS",
        str(Path(__file__).parent.parent.parent.parent.parent / "credentials" / "dbt-sa-key.json")
    )
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    
    project_id = os.getenv("GCP_PROJECT", "graphic-boulder-483814-g7")
    return bigquery.Client(project=project_id)


# ============================================
# BIGQUERY RAW DATA ASSETS
# ============================================

@asset(
    group_name="raw_bigquery",
    description="Raw listen events from GCS (BigQuery external table)",
    compute_kind="bigquery"
)
def bq_raw_listen_events(context: AssetExecutionContext) -> Output[int]:
    """
    Check raw listen events in BigQuery external table.
    Data comes from GCS Parquet files written by Spark.
    """
    client = get_bigquery_client()
    
    query = """
        SELECT COUNT(*) as count
        FROM `raw.ext_listen_events`
    """
    
    result = client.query(query).result()
    count = list(result)[0].count
    
    context.log.info(f"raw.ext_listen_events: {count} rows")
    
    return Output(
        count,
        metadata={
            "row_count": MetadataValue.int(count),
            "table": MetadataValue.text("raw.ext_listen_events"),
            "source": MetadataValue.text("GCS Parquet (external table)")
        }
    )


@asset(
    group_name="raw_bigquery",
    description="Raw page view events from GCS",
    compute_kind="bigquery"
)
def bq_raw_page_view_events(context: AssetExecutionContext) -> Output[int]:
    """Check raw page view events in BigQuery"""
    client = get_bigquery_client()
    
    query = """
        SELECT COUNT(*) as count
        FROM `raw.ext_page_view_events`
    """
    
    result = client.query(query).result()
    count = list(result)[0].count
    
    context.log.info(f"raw.ext_page_view_events: {count} rows")
    
    return Output(
        count,
        metadata={
            "row_count": MetadataValue.int(count),
            "table": MetadataValue.text("raw.ext_page_view_events"),
        }
    )


@asset(
    group_name="raw_bigquery",
    description="Raw auth events from GCS",
    compute_kind="bigquery"
)
def bq_raw_auth_events(context: AssetExecutionContext) -> Output[int]:
    """Check raw auth events in BigQuery"""
    client = get_bigquery_client()
    
    query = """
        SELECT COUNT(*) as count
        FROM `raw.ext_auth_events`
    """
    
    result = client.query(query).result()
    count = list(result)[0].count
    
    context.log.info(f"raw.ext_auth_events: {count} rows")
    
    return Output(
        count,
        metadata={
            "row_count": MetadataValue.int(count),
            "table": MetadataValue.text("raw.ext_auth_events"),
        }
    )


@asset(
    group_name="raw_bigquery",
    description="Raw status change events from GCS",
    compute_kind="bigquery"
)
def bq_raw_status_change_events(context: AssetExecutionContext) -> Output[int]:
    """Check raw status change events in BigQuery"""
    client = get_bigquery_client()
    
    query = """
        SELECT COUNT(*) as count
        FROM `raw.ext_status_change_events`
    """
    
    result = client.query(query).result()
    count = list(result)[0].count
    
    context.log.info(f"raw.ext_status_change_events: {count} rows")
    
    return Output(
        count,
        metadata={
            "row_count": MetadataValue.int(count),
            "table": MetadataValue.text("raw.ext_status_change_events"),
        }
    )


# ============================================
# BIGQUERY MARTS VALIDATION ASSETS
# ============================================

@asset(
    group_name="marts_validation",
    description="Validate mart_top_songs in BigQuery",
    compute_kind="bigquery"
)
def validate_mart_top_songs(context: AssetExecutionContext) -> Output[dict]:
    """Validate mart_top_songs table has data"""
    client = get_bigquery_client()
    
    query = """
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT song) as unique_songs,
            COUNT(DISTINCT artist) as unique_artists,
            MAX(updated_at) as last_updated
        FROM `staging_marts.mart_top_songs`
    """
    
    result = list(client.query(query).result())[0]
    
    stats = {
        "total_rows": result.total_rows,
        "unique_songs": result.unique_songs,
        "unique_artists": result.unique_artists,
        "last_updated": str(result.last_updated) if result.last_updated else None
    }
    
    context.log.info(f"mart_top_songs stats: {stats}")
    
    return Output(
        stats,
        metadata={
            "total_rows": MetadataValue.int(stats["total_rows"]),
            "unique_songs": MetadataValue.int(stats["unique_songs"]),
            "unique_artists": MetadataValue.int(stats["unique_artists"]),
            "last_updated": MetadataValue.text(stats["last_updated"] or "N/A"),
        }
    )


@asset(
    group_name="marts_validation",
    description="Validate mart_active_users in BigQuery",
    compute_kind="bigquery"
)
def validate_mart_active_users(context: AssetExecutionContext) -> Output[dict]:
    """Validate mart_active_users table"""
    client = get_bigquery_client()
    
    query = """
        SELECT 
            COUNT(*) as total_users,
            COUNTIF(engagement_tier = 'Power User') as power_users,
            COUNTIF(engagement_tier = 'Active') as active_users,
            COUNTIF(engagement_tier = 'Casual') as casual_users,
            COUNTIF(engagement_tier = 'New') as new_users,
            COUNTIF(is_active) as currently_active
        FROM `staging_marts.mart_active_users`
    """
    
    result = list(client.query(query).result())[0]
    
    stats = {
        "total_users": result.total_users,
        "power_users": result.power_users,
        "active_users": result.active_users,
        "casual_users": result.casual_users,
        "new_users": result.new_users,
        "currently_active": result.currently_active
    }
    
    context.log.info(f"mart_active_users stats: {stats}")
    
    return Output(
        stats,
        metadata={
            "total_users": MetadataValue.int(stats["total_users"]),
            "engagement_breakdown": MetadataValue.md(
                f"| Tier | Count |\n|------|-------|\n"
                f"| Power | {stats['power_users']} |\n"
                f"| Active | {stats['active_users']} |\n"
                f"| Casual | {stats['casual_users']} |\n"
                f"| New | {stats['new_users']} |"
            ),
        }
    )
