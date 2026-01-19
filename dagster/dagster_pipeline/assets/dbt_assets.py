"""
dbt Assets for SoundFlow Pipeline
==================================
Supports both local (PostgreSQL) and production (BigQuery) environments.
"""

import os
import subprocess
from pathlib import Path

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    Output,
    Config,
)

# ============================================
# CONFIGURATION
# ============================================

class DbtConfig(Config):
    """dbt configuration"""
    target: str = "local"  # "local" for PostgreSQL, "prod" for BigQuery


def get_dbt_env(target: str = "local") -> dict:
    """Get environment variables for dbt"""
    env = {**os.environ}
    
    if target == "local":
        env.update({
            "DBT_TARGET": "local",
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "localhost"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB", "soundflow"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", "soundflow"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "soundflow123"),
        })
    elif target == "prod":
        env.update({
            "DBT_TARGET": "prod",
            "GCP_PROJECT": os.getenv("GCP_PROJECT", "graphic-boulder-483814-g7"),
            "BQ_LOCATION": os.getenv("BQ_LOCATION", "asia-southeast1"),
            "GOOGLE_APPLICATION_CREDENTIALS": os.getenv(
                "GOOGLE_APPLICATION_CREDENTIALS",
                str(Path(__file__).parent.parent.parent.parent.parent / "credentials" / "dbt-sa-key.json")
            ),
        })
    
    return env


def get_dbt_project_dir() -> Path:
    """Get dbt project directory"""
    # Check for Docker/container environment first
    container_path = Path("/opt/dagster/dbt")
    if container_path.exists():
        return container_path
    
    # Local development
    return Path(__file__).parent.parent.parent.parent.parent / "dbt"


def run_dbt_command(args: list, context: AssetExecutionContext, target: str = "local") -> dict:
    """Run a dbt command and return results"""
    dbt_dir = get_dbt_project_dir()
    env = get_dbt_env(target)
    
    cmd = ["dbt"] + args + ["--profiles-dir", str(dbt_dir), "--project-dir", str(dbt_dir)]
    
    context.log.info(f"Running dbt command: {' '.join(cmd)}")
    context.log.info(f"Target: {target}")
    context.log.info(f"Project dir: {dbt_dir}")
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
        cwd=str(dbt_dir)
    )
    
    if result.stdout:
        context.log.info(result.stdout)
    
    if result.returncode != 0 and result.stderr:
        context.log.error(result.stderr)
        
    return {
        "success": result.returncode == 0,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "return_code": result.returncode,
        "target": target
    }


# ============================================
# DBT ASSETS
# ============================================

@asset(
    group_name="dbt_staging",
    description="dbt staging models - clean and standardize raw data",
    compute_kind="dbt",
    metadata={
        "models": ["stg_listens", "stg_page_views", "stg_auth", "stg_status_changes"]
    }
)
def dbt_staging_models(context: AssetExecutionContext, config: DbtConfig) -> Output[dict]:
    """
    Run dbt staging models.
    
    Transforms raw event data into clean, standardized staging tables:
    - stg_listens: Listen events with derived time fields
    - stg_page_views: Page view events
    - stg_auth: Authentication events
    - stg_status_changes: Subscription status changes
    """
    result = run_dbt_command(
        ["run", "--select", "staging"],
        context,
        target=config.target
    )
    
    if not result["success"]:
        raise Exception(f"dbt staging failed: {result['stderr']}")
    
    return Output(
        result,
        metadata={
            "status": MetadataValue.text("success"),
            "target": MetadataValue.text(config.target),
            "models_run": MetadataValue.text("stg_listens, stg_page_views, stg_auth, stg_status_changes")
        }
    )


@asset(
    group_name="dbt_intermediate",
    deps=["dbt_staging_models"],
    description="dbt intermediate models - aggregations and metrics",
    compute_kind="dbt",
    metadata={
        "models": ["int_song_stats", "int_user_activity", "int_daily_metrics"]
    }
)
def dbt_intermediate_models(context: AssetExecutionContext, config: DbtConfig) -> Output[dict]:
    """
    Run dbt intermediate models.
    
    Creates intermediate aggregations:
    - int_song_stats: Song-level statistics
    - int_user_activity: User activity summaries
    - int_daily_metrics: Daily aggregated metrics
    """
    result = run_dbt_command(
        ["run", "--select", "intermediate"],
        context,
        target=config.target
    )
    
    if not result["success"]:
        raise Exception(f"dbt intermediate failed: {result['stderr']}")
    
    return Output(
        result,
        metadata={
            "status": MetadataValue.text("success"),
            "target": MetadataValue.text(config.target),
            "models_run": MetadataValue.text("int_song_stats, int_user_activity, int_daily_metrics")
        }
    )


@asset(
    group_name="dbt_marts",
    deps=["dbt_intermediate_models"],
    description="dbt marts models - business-ready analytics tables",
    compute_kind="dbt",
    metadata={
        "models": ["mart_top_songs", "mart_top_artists", "mart_active_users", 
                   "mart_daily_summary", "mart_hourly_metrics", "mart_location_analytics"]
    }
)
def dbt_marts_models(context: AssetExecutionContext, config: DbtConfig) -> Output[dict]:
    """
    Run dbt marts models.
    
    Creates business-ready analytics tables:
    - mart_top_songs: Song rankings and statistics
    - mart_top_artists: Artist rankings
    - mart_active_users: User engagement metrics
    - mart_daily_summary: Daily KPIs
    - mart_hourly_metrics: Hourly trends
    - mart_location_analytics: Geographic analytics
    """
    result = run_dbt_command(
        ["run", "--select", "marts"],
        context,
        target=config.target
    )
    
    if not result["success"]:
        raise Exception(f"dbt marts failed: {result['stderr']}")
    
    return Output(
        result,
        metadata={
            "status": MetadataValue.text("success"),
            "target": MetadataValue.text(config.target),
            "models_run": MetadataValue.text("mart_top_songs, mart_top_artists, mart_active_users, mart_daily_summary, mart_hourly_metrics, mart_location_analytics")
        }
    )


@asset(
    group_name="dbt_quality",
    deps=["dbt_marts_models"],
    description="dbt tests - data quality validation",
    compute_kind="dbt"
)
def dbt_test_results(context: AssetExecutionContext, config: DbtConfig) -> Output[dict]:
    """
    Run dbt tests for data quality validation.
    
    Tests include:
    - Not null constraints
    - Unique constraints
    - Referential integrity
    - Accepted values
    """
    result = run_dbt_command(
        ["test"],
        context,
        target=config.target
    )
    
    # Parse test results from output
    stdout = result.get("stdout", "")
    passed = stdout.count("PASS")
    failed = stdout.count("FAIL")
    
    return Output(
        result,
        metadata={
            "tests_passed": MetadataValue.bool(result["success"]),
            "target": MetadataValue.text(config.target),
            "pass_count": MetadataValue.int(passed),
            "fail_count": MetadataValue.int(failed),
            "summary": MetadataValue.md(f"**Tests:** {passed} passed, {failed} failed")
        }
    )


@asset(
    group_name="dbt_docs",
    deps=["dbt_test_results"],
    description="Generate dbt documentation",
    compute_kind="dbt"
)
def dbt_docs(context: AssetExecutionContext, config: DbtConfig) -> Output[dict]:
    """
    Generate dbt documentation.
    """
    result = run_dbt_command(
        ["docs", "generate"],
        context,
        target=config.target
    )
    
    return Output(
        result,
        metadata={
            "status": MetadataValue.text("success" if result["success"] else "failed"),
            "target": MetadataValue.text(config.target),
        }
    )
