"""
Spark Assets for SoundFlow Pipeline
====================================
Assets for monitoring and triggering Spark streaming jobs.
"""

import os
from datetime import datetime

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    Output,
)


@asset(
    group_name="spark",
    description="Check Spark streaming job status on Dataproc",
    compute_kind="spark"
)
def spark_job_status(context: AssetExecutionContext) -> Output[dict]:
    """
    Check the status of Spark streaming jobs on Dataproc.
    
    This asset queries the Dataproc API to get job status.
    """
    from google.cloud import dataproc_v1
    
    project_id = os.getenv("GCP_PROJECT", "graphic-boulder-483814-g7")
    region = os.getenv("GCP_REGION", "asia-southeast1")
    cluster_name = os.getenv("DATAPROC_CLUSTER", "soundflow-spark-dev")
    
    # Check cluster status
    cluster_client = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )
    
    try:
        cluster = cluster_client.get_cluster(
            project_id=project_id,
            region=region,
            cluster_name=cluster_name
        )
        
        cluster_status = cluster.status.state.name
        context.log.info(f"Cluster {cluster_name} status: {cluster_status}")
        
        status = {
            "cluster_name": cluster_name,
            "cluster_status": cluster_status,
            "cluster_running": cluster_status == "RUNNING",
            "checked_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        context.log.warning(f"Could not get cluster status: {e}")
        status = {
            "cluster_name": cluster_name,
            "cluster_status": "UNKNOWN",
            "cluster_running": False,
            "error": str(e),
            "checked_at": datetime.now().isoformat()
        }
    
    return Output(
        status,
        metadata={
            "cluster": MetadataValue.text(cluster_name),
            "status": MetadataValue.text(status["cluster_status"]),
            "running": MetadataValue.bool(status["cluster_running"]),
        }
    )


@asset(
    group_name="spark",
    description="GCS data freshness check",
    compute_kind="gcs"
)
def gcs_data_freshness(context: AssetExecutionContext) -> Output[dict]:
    """
    Check the freshness of data in GCS.
    
    Looks at the latest partition timestamp to determine
    how recent the data is.
    """
    from google.cloud import storage
    
    project_id = os.getenv("GCP_PROJECT", "graphic-boulder-483814-g7")
    bucket_name = os.getenv("GCS_BUCKET", "tf-state-soundflow-123")
    
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    
    # Check latest files for each event type
    event_types = ["listen_events", "page_view_events", "auth_events", "status_change_events"]
    
    freshness = {}
    for event_type in event_types:
        prefix = f"raw/{event_type}/"
        blobs = list(bucket.list_blobs(prefix=prefix, max_results=100))
        
        if blobs:
            # Get latest modified time
            latest = max(blobs, key=lambda b: b.updated)
            freshness[event_type] = {
                "latest_file": latest.name,
                "updated_at": latest.updated.isoformat(),
                "file_count": len(blobs)
            }
            context.log.info(f"{event_type}: {len(blobs)} files, latest: {latest.updated}")
        else:
            freshness[event_type] = {
                "latest_file": None,
                "updated_at": None,
                "file_count": 0
            }
            context.log.warning(f"{event_type}: No files found")
    
    return Output(
        freshness,
        metadata={
            "bucket": MetadataValue.text(bucket_name),
            "event_types_checked": MetadataValue.int(len(event_types)),
            "freshness_summary": MetadataValue.json(freshness)
        }
    )
