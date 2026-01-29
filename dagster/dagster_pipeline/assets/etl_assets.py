"""
ETL Assets for SoundFlow Pipeline
==================================
Assets for running Kafka to GCS ETL pipeline.
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    Output,
)


def is_docker_environment() -> bool:
    """Check if running inside Docker container"""
    # Check common Docker indicators
    return (
        os.path.exists("/.dockerenv") or
        os.path.exists("/opt/dagster/credentials") or
        os.getenv("DAGSTER_HOME", "").startswith("/opt")
    )


def get_project_root() -> Path:
    """Get project root directory - handles both Docker and local environments"""
    if is_docker_environment():
        # Docker: credentials mounted at /opt/dagster/credentials
        # Project structure: /opt/dagster/dagster_pipeline (code) + /opt/dagster/dbt (dbt)
        return Path("/opt/dagster")
    
    # Local development - dagster/dagster_pipeline/assets/etl_assets.py
    # Go up 4 levels: assets -> dagster_pipeline -> dagster -> project_root
    return Path(__file__).parent.parent.parent.parent.resolve()


def get_etl_script_path() -> Path:
    """Get path to kafka_to_gcs_python.py"""
    if is_docker_environment():
        # In Docker, spark_streaming is mounted separately or use Python path
        return Path("/opt/dagster/spark_streaming/src/kafka_to_gcs_python.py")
    return get_project_root() / "spark_streaming" / "src" / "kafka_to_gcs_python.py"


def get_credentials_path() -> Path:
    """Get path to GCP credentials file"""
    # 1. Check environment variable first
    env_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if env_creds and Path(env_creds).exists():
        return Path(env_creds)
    
    # 2. Check Docker path
    docker_creds = Path("/opt/dagster/credentials/pipeline-sa-key.json")
    if docker_creds.exists():
        return docker_creds
    
    # 3. Fall back to local path
    return get_project_root() / "credentials" / "pipeline-sa-key.json"


def get_kafka_bootstrap_servers() -> str:
    """Get Kafka bootstrap servers - handles Docker vs local networking"""
    # 1. Environment variable takes priority
    env_kafka = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if env_kafka:
        return env_kafka
    
    # 2. Docker uses internal network name
    if is_docker_environment():
        return "redpanda:29092"
    
    # 3. Local development uses localhost
    return "localhost:9092"


def get_python_env() -> dict:
    """Get environment variables for ETL script"""
    env = {**os.environ}
    
    credentials_path = get_credentials_path()
    kafka_servers = get_kafka_bootstrap_servers()
    
    env.update({
        "GCP_PROJECT": os.getenv("GCP_PROJECT", "graphic-boulder-483814-g7"),
        "GCS_BUCKET": os.getenv("GCS_BUCKET", "tf-state-soundflow-123"),
        "KAFKA_BOOTSTRAP_SERVERS": kafka_servers,
        "GOOGLE_APPLICATION_CREDENTIALS": str(credentials_path),
        # Fix Windows encoding issues
        "PYTHONIOENCODING": "utf-8",
        "PYTHONLEGACYWINDOWSSTDIO": "utf-8",
    })
    
    return env


@asset(
    group_name="etl",
    description="Transfer data from Kafka to GCS as Parquet files",
    compute_kind="python"
)
def kafka_to_gcs_transfer(context: AssetExecutionContext) -> Output[dict]:
    """
    Run the Kafka to GCS ETL script.
    
    This asset executes kafka_to_gcs_python.py which:
    1. Consumes messages from Kafka topics
    2. Converts to Parquet format
    3. Uploads to GCS with Hive-style partitioning
    
    Returns:
        dict with transfer statistics
    """
    script_path = get_etl_script_path()
    env = get_python_env()
    
    context.log.info(f"Running ETL script: {script_path}")
    context.log.info(f"Kafka bootstrap servers: {env.get('KAFKA_BOOTSTRAP_SERVERS')}")
    context.log.info(f"GCS bucket: {env.get('GCS_BUCKET')}")
    context.log.info(f"Credentials: {env.get('GOOGLE_APPLICATION_CREDENTIALS')}")
    
    start_time = datetime.now()
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            env=env,
            cwd=str(script_path.parent),
            timeout=300  # 5 minute timeout (reduced from 10)
        )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Parse output to extract statistics
        stdout = result.stdout
        stderr = result.stderr
        
        context.log.info(f"ETL stdout:\n{stdout}")
        
        if result.returncode != 0:
            context.log.error(f"ETL stderr:\n{stderr}")
            raise Exception(f"ETL script failed with return code {result.returncode}")
        
        # Extract record counts from output
        stats = {
            "success": True,
            "duration_seconds": duration,
            "stdout": stdout,
            "return_code": result.returncode,
            "completed_at": end_time.isoformat(),
        }
        
        # Try to parse record counts from output
        for line in stdout.split('\n'):
            if 'Processed' in line and 'records' in line:
                context.log.info(f"Processed: {line}")
        
        return Output(
            stats,
            metadata={
                "duration_seconds": MetadataValue.float(duration),
                "success": MetadataValue.bool(True),
                "completed_at": MetadataValue.text(end_time.isoformat()),
            }
        )
        
    except subprocess.TimeoutExpired:
        context.log.error("ETL script timed out after 10 minutes")
        return Output(
            {"success": False, "error": "Timeout"},
            metadata={"success": MetadataValue.bool(False)}
        )
    except Exception as e:
        context.log.error(f"ETL script failed: {e}")
        return Output(
            {"success": False, "error": str(e)},
            metadata={"success": MetadataValue.bool(False), "error": MetadataValue.text(str(e))}
        )


@asset(
    group_name="etl",
    description="Check if Kafka/Redpanda is running and has data",
    compute_kind="kafka"
)
def kafka_health_check(context: AssetExecutionContext) -> Output[dict]:
    """
    Check Kafka/Redpanda health and topic status.
    
    Returns:
        dict with Kafka connection status and topic info
    """
    from confluent_kafka import Consumer, KafkaException
    from confluent_kafka.admin import AdminClient
    
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    
    context.log.info(f"Checking Kafka at {bootstrap_servers}")
    
    try:
        # Create admin client to list topics
        admin_config = {'bootstrap.servers': bootstrap_servers}
        admin = AdminClient(admin_config)
        
        # Get cluster metadata
        cluster_metadata = admin.list_topics(timeout=10)
        
        topics = {}
        expected_topics = ['listen_events', 'page_view_events', 'auth_events', 'status_change_events']
        
        for topic_name in expected_topics:
            if topic_name in cluster_metadata.topics:
                topic_metadata = cluster_metadata.topics[topic_name]
                topics[topic_name] = {
                    "exists": True,
                    "partitions": len(topic_metadata.partitions),
                }
                context.log.info(f"Topic {topic_name}: {len(topic_metadata.partitions)} partitions")
            else:
                topics[topic_name] = {"exists": False, "partitions": 0}
                context.log.warning(f"Topic {topic_name}: NOT FOUND")
        
        status = {
            "connected": True,
            "broker": bootstrap_servers,
            "topics": topics,
            "checked_at": datetime.now().isoformat()
        }
        
        return Output(
            status,
            metadata={
                "connected": MetadataValue.bool(True),
                "broker": MetadataValue.text(bootstrap_servers),
                "topics_found": MetadataValue.int(sum(1 for t in topics.values() if t["exists"])),
            }
        )
        
    except Exception as e:
        context.log.error(f"Kafka connection failed: {e}")
        return Output(
            {
                "connected": False,
                "broker": bootstrap_servers,
                "error": str(e),
                "checked_at": datetime.now().isoformat()
            },
            metadata={
                "connected": MetadataValue.bool(False),
                "error": MetadataValue.text(str(e)),
            }
        )


@asset(
    group_name="etl",
    deps=["kafka_health_check"],
    description="Generate events using EventSim (Docker)",
    compute_kind="docker"
)
def eventsim_generate(context: AssetExecutionContext) -> Output[dict]:
    """
    Trigger EventSim to generate events.
    
    This runs docker-compose to start EventSim container.
    
    Note: This is optional - EventSim can also run independently.
    """
    project_root = get_project_root()
    compose_file = project_root / "docker-compose.yml"
    
    context.log.info(f"Starting EventSim from {compose_file}")
    
    try:
        # Check if Redpanda is running first
        result = subprocess.run(
            ["docker-compose", "ps", "redpanda"],
            capture_output=True,
            text=True,
            cwd=str(project_root),
            timeout=30
        )
        
        if "running" not in result.stdout.lower() and "Up" not in result.stdout:
            context.log.info("Starting Redpanda...")
            subprocess.run(
                ["docker-compose", "up", "-d", "redpanda"],
                capture_output=True,
                text=True,
                cwd=str(project_root),
                timeout=60
            )
        
        # Run EventSim
        context.log.info("Running EventSim...")
        result = subprocess.run(
            ["docker-compose", "up", "eventsim"],
            capture_output=True,
            text=True,
            cwd=str(project_root),
            timeout=1800  # 30 minute timeout for event generation
        )
        
        return Output(
            {
                "success": result.returncode == 0,
                "stdout": result.stdout[-2000:] if result.stdout else "",  # Last 2000 chars
                "completed_at": datetime.now().isoformat()
            },
            metadata={
                "success": MetadataValue.bool(result.returncode == 0),
            }
        )
        
    except subprocess.TimeoutExpired:
        context.log.warning("EventSim timed out - this may be normal for large datasets")
        return Output(
            {"success": True, "note": "Timed out but may have completed"},
            metadata={"success": MetadataValue.bool(True)}
        )
    except Exception as e:
        context.log.error(f"EventSim failed: {e}")
        return Output(
            {"success": False, "error": str(e)},
            metadata={"success": MetadataValue.bool(False)}
        )
