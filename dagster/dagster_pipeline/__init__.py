"""
SoundFlow Dagster Pipeline
===========================
Orchestrates dbt transformations for the music streaming data pipeline.

Pipeline Flow:
1. Check raw data availability
2. Run dbt transformations (staging → intermediate → marts)
3. Run dbt tests for data quality
4. Report status

Schedule: Every hour at minute 15
"""

import os
import subprocess
from pathlib import Path

from dagster import (
    Definitions,
    asset,
    AssetExecutionContext,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
    sensor,
    RunRequest,
    SensorEvaluationContext,
    DefaultSensorStatus,
    MetadataValue,
    Output,
    op,
    job,
    schedule,
    In,
    Out,
    graph,
)

# ============================================
# CONFIGURATION
# ============================================

# dbt project path - in Docker container
DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", "/opt/dagster/dbt"))
DBT_PROFILES_DIR = DBT_PROJECT_DIR

# Environment
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "soundflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "soundflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "soundflow123")


def get_db_connection():
    """Create PostgreSQL connection"""
    import psycopg2
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def run_dbt_command(args: list, context) -> dict:
    """Run a dbt command and return results"""
    env = {
        **os.environ,
        "DBT_TARGET": "local",
        "POSTGRES_HOST": POSTGRES_HOST,
        "POSTGRES_PORT": POSTGRES_PORT,
        "POSTGRES_DB": POSTGRES_DB,
        "POSTGRES_USER": POSTGRES_USER,
        "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
    }
    
    cmd = ["dbt"] + args + ["--profiles-dir", str(DBT_PROFILES_DIR)]
    
    context.log.info(f"Running: {' '.join(cmd)}")
    
    result = subprocess.run(
        cmd,
        cwd=str(DBT_PROJECT_DIR),
        capture_output=True,
        text=True,
        env=env
    )
    
    context.log.info(result.stdout)
    
    if result.returncode != 0:
        context.log.error(result.stderr)
        
    return {
        "success": result.returncode == 0,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "return_code": result.returncode
    }


# ============================================
# ASSETS - Raw Data Sources
# ============================================

@asset(
    group_name="raw",
    description="Raw listen events from Spark Streaming",
    compute_kind="postgres"
)
def raw_listen_events(context: AssetExecutionContext) -> Output[int]:
    """Check raw listen events table"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM raw.listen_events")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    
    context.log.info(f"raw.listen_events: {count} rows")
    
    return Output(
        count,
        metadata={
            "row_count": MetadataValue.int(count),
            "table": MetadataValue.text("raw.listen_events")
        }
    )


@asset(
    group_name="raw",
    description="Raw status change events",
    compute_kind="postgres"
)
def raw_status_change_events(context: AssetExecutionContext) -> Output[int]:
    """Check raw status change events table"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM raw.status_change_events")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    
    context.log.info(f"raw.status_change_events: {count} rows")
    
    return Output(
        count,
        metadata={
            "row_count": MetadataValue.int(count),
            "table": MetadataValue.text("raw.status_change_events")
        }
    )


# ============================================
# ASSETS - dbt Models
# ============================================

@asset(
    group_name="staging",
    deps=[raw_listen_events, raw_status_change_events],
    description="dbt staging models",
    compute_kind="dbt"
)
def dbt_staging(context: AssetExecutionContext) -> Output[dict]:
    """Run dbt staging models"""
    result = run_dbt_command(["run", "--select", "staging"], context)
    
    if not result["success"]:
        raise Exception(f"dbt staging failed: {result['stderr']}")
    
    return Output(
        result,
        metadata={
            "status": MetadataValue.text("success" if result["success"] else "failed"),
            "models": MetadataValue.text("stg_listens, stg_page_views, stg_auth, stg_status_changes")
        }
    )


@asset(
    group_name="intermediate",
    deps=[dbt_staging],
    description="dbt intermediate models",
    compute_kind="dbt"
)
def dbt_intermediate(context: AssetExecutionContext) -> Output[dict]:
    """Run dbt intermediate models"""
    result = run_dbt_command(["run", "--select", "intermediate"], context)
    
    if not result["success"]:
        raise Exception(f"dbt intermediate failed: {result['stderr']}")
    
    return Output(
        result,
        metadata={
            "status": MetadataValue.text("success" if result["success"] else "failed"),
            "models": MetadataValue.text("int_song_stats, int_user_activity, int_daily_metrics")
        }
    )


@asset(
    group_name="marts",
    deps=[dbt_intermediate],
    description="dbt marts models",
    compute_kind="dbt"
)
def dbt_marts(context: AssetExecutionContext) -> Output[dict]:
    """Run dbt marts models"""
    result = run_dbt_command(["run", "--select", "marts"], context)
    
    if not result["success"]:
        raise Exception(f"dbt marts failed: {result['stderr']}")
    
    return Output(
        result,
        metadata={
            "status": MetadataValue.text("success" if result["success"] else "failed"),
            "models": MetadataValue.text("mart_top_songs, mart_active_users, mart_location_analytics, mart_hourly_metrics, mart_top_artists, mart_daily_summary")
        }
    )


@asset(
    group_name="quality",
    deps=[dbt_marts],
    description="dbt tests for data quality",
    compute_kind="dbt"
)
def dbt_tests(context: AssetExecutionContext) -> Output[dict]:
    """Run dbt tests"""
    result = run_dbt_command(["test"], context)
    
    return Output(
        result,
        metadata={
            "tests_passed": MetadataValue.bool(result["success"]),
            "output": MetadataValue.text(result["stdout"][:500] if result["stdout"] else "No output")
        }
    )


# ============================================
# JOBS
# ============================================

# Job to run all dbt transformations (full refresh for ranking tables)
dbt_full_refresh_job = define_asset_job(
    name="dbt_full_refresh",
    selection=[
        raw_listen_events,
        raw_status_change_events,
        dbt_staging,
        dbt_intermediate,
        dbt_marts,
        dbt_tests
    ],
    description="Run complete dbt transformation pipeline (full refresh)"
)

# Job to run only marts (incremental - fast updates)
dbt_incremental_job = define_asset_job(
    name="dbt_incremental",
    selection=[dbt_marts, dbt_tests],
    description="Run incremental marts update (fast, for near real-time)"
)


# ============================================
# SCHEDULES
# ============================================

# Every 5 minutes - incremental update for time-series marts
frequent_incremental_schedule = ScheduleDefinition(
    job=dbt_incremental_job,
    cron_schedule="*/5 * * * *",  # Every 5 minutes
    name="frequent_incremental_update",
    description="Run incremental marts every 5 minutes for near real-time analytics"
)

# Hourly schedule - run full pipeline at minute 15 every hour
hourly_dbt_schedule = ScheduleDefinition(
    job=dbt_full_refresh_job,
    cron_schedule="15 * * * *",  # Every hour at :15
    name="hourly_dbt_transformations",
    description="Run dbt transformations every hour at minute 15"
)

# Daily schedule - run at 2 AM
daily_dbt_schedule = ScheduleDefinition(
    job=dbt_full_refresh_job,
    cron_schedule="0 2 * * *",  # 2 AM daily
    name="daily_dbt_transformations",
    description="Run full dbt transformations at 2 AM daily"
)


# ============================================
# SENSORS
# ============================================

@sensor(
    job=dbt_incremental_job,
    minimum_interval_seconds=60,  # Check every 1 minute
    default_status=DefaultSensorStatus.STOPPED,  # Start manually via UI
)
def new_data_sensor(context: SensorEvaluationContext):
    """
    Sensor that triggers incremental dbt when new data arrives.
    
    - Polls PostgreSQL every 60 seconds
    - Checks latest timestamp in raw.listen_events
    - Triggers incremental job if new data detected
    - Uses cursor to track last processed timestamp
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get latest event timestamp
        cursor.execute("""
            SELECT MAX(event_timestamp) 
            FROM raw.listen_events
        """)
        latest_ts = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        if latest_ts is None:
            context.log.info("No data in raw.listen_events yet")
            return
        
        latest_ts_str = str(latest_ts)
        
        # Check if this is new data
        last_processed = context.cursor
        
        if last_processed is None or latest_ts_str > last_processed:
            context.log.info(f"New data detected: {latest_ts_str}")
            yield RunRequest(
                run_key=latest_ts_str,
                run_config={},
            )
            context.update_cursor(latest_ts_str)
        else:
            context.log.info(f"No new data. Last: {last_processed}, Current: {latest_ts_str}")
            
    except Exception as e:
        context.log.error(f"Sensor error: {e}")


# ============================================
# DEFINITIONS (Main entry point)
# ============================================

defs = Definitions(
    assets=[
        raw_listen_events,
        raw_status_change_events,
        dbt_staging,
        dbt_intermediate,
        dbt_marts,
        dbt_tests,
    ],
    jobs=[
        dbt_full_refresh_job,
        dbt_incremental_job,
    ],
    schedules=[
        frequent_incremental_schedule,  # Every 5 min - incremental
        hourly_dbt_schedule,            # Hourly - full refresh
        daily_dbt_schedule,             # Daily at 2 AM - full refresh
    ],
    sensors=[
        new_data_sensor,
    ],
)
