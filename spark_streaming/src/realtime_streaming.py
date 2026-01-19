"""
Real-time Spark Structured Streaming for GCP
=============================================
Continuous streaming from Pub/Sub to GCS with:
- Near real-time processing (10 second micro-batches)
- Exactly-once semantics with checkpointing
- Auto-recovery from failures
- Graceful shutdown handling

Usage on Dataproc:
    gcloud dataproc jobs submit pyspark realtime_streaming.py \
        --cluster=soundflow-spark-dev \
        --region=asia-southeast1 \
        --py-files=config.py \
        --properties="spark.jars.packages=com.google.cloud.spark:spark-3.3-bigquery:0.30.0" \
        -- --mode continuous
"""

import os
import sys
import argparse
import signal
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, year, month, dayofmonth, hour,
    current_timestamp, expr, window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType, BooleanType
)


# ============================================
# CONFIGURATION
# ============================================

GCP_PROJECT = os.getenv("GCP_PROJECT", "graphic-boulder-483814-g7")
GCS_BUCKET = os.getenv("GCS_BUCKET", "tf-state-soundflow-123")
GCP_REGION = os.getenv("GCP_REGION", "asia-southeast1")

# Processing configuration
TRIGGER_INTERVAL_CONTINUOUS = "10 seconds"  # Near real-time
TRIGGER_INTERVAL_MICRO_BATCH = "1 minute"   # Standard micro-batch
TRIGGER_INTERVAL_BATCH = "5 minutes"        # Batch-like processing

# Pub/Sub settings
PUBSUB_SUBSCRIPTION_PREFIX = "spark"


# ============================================
# EVENT SCHEMAS
# ============================================

LISTEN_EVENT_SCHEMA = StructType([
    StructField("ts", LongType(), True),
    StructField("userId", StringType(), True),
    StructField("sessionId", LongType(), True),
    StructField("page", StringType(), True),
    StructField("auth", StringType(), True),
    StructField("method", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("song", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", LongType(), True),
    StructField("location", StringType(), True),
    StructField("userAgent", StringType(), True),
])

PAGE_VIEW_EVENT_SCHEMA = StructType([
    StructField("ts", LongType(), True),
    StructField("userId", StringType(), True),
    StructField("sessionId", LongType(), True),
    StructField("page", StringType(), True),
    StructField("auth", StringType(), True),
    StructField("method", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", LongType(), True),
    StructField("location", StringType(), True),
    StructField("userAgent", StringType(), True),
])

AUTH_EVENT_SCHEMA = StructType([
    StructField("ts", LongType(), True),
    StructField("userId", StringType(), True),
    StructField("sessionId", LongType(), True),
    StructField("page", StringType(), True),
    StructField("auth", StringType(), True),
    StructField("method", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("level", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", LongType(), True),
    StructField("location", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("success", BooleanType(), True),
])

STATUS_CHANGE_SCHEMA = StructType([
    StructField("ts", LongType(), True),
    StructField("userId", StringType(), True),
    StructField("sessionId", LongType(), True),
    StructField("auth", StringType(), True),
    StructField("level", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", LongType(), True),
    StructField("location", StringType(), True),
    StructField("userAgent", StringType(), True),
])

EVENT_CONFIGS = {
    "listen_events": {
        "schema": LISTEN_EVENT_SCHEMA,
        "subscription": "listen-events-spark-sub",
        "output_path": "raw/listen_events",
    },
    "page_view_events": {
        "schema": PAGE_VIEW_EVENT_SCHEMA,
        "subscription": "page-view-events-spark-sub",
        "output_path": "raw/page_view_events",
    },
    "auth_events": {
        "schema": AUTH_EVENT_SCHEMA,
        "subscription": "auth-events-spark-sub",
        "output_path": "raw/auth_events",
    },
    "status_change_events": {
        "schema": STATUS_CHANGE_SCHEMA,
        "subscription": "status-change-events-spark-sub",
        "output_path": "raw/status_change_events",
    },
}


class GracefulShutdown:
    """Handle graceful shutdown signals"""
    
    def __init__(self):
        self.shutdown_requested = False
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        print(f"\n[{datetime.now()}] Shutdown signal received. Stopping gracefully...")
        self.shutdown_requested = True


def create_spark_session(app_name: str = "SoundFlowRealTimeStreaming") -> SparkSession:
    """Create optimized Spark session for streaming"""
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.streaming.checkpointLocation", f"gs://{GCS_BUCKET}/checkpoints/streaming") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
        .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_pubsub_stream(spark: SparkSession, subscription: str):
    """
    Read streaming data from Pub/Sub.
    
    Note: For Dataproc, the Pub/Sub connector reads messages automatically.
    The connector handles acknowledgment after successful write.
    """
    full_subscription = f"projects/{GCP_PROJECT}/subscriptions/{subscription}"
    
    print(f"  Reading from: {full_subscription}")
    
    # Use spark-pubsub connector
    return spark.readStream \
        .format("pubsub") \
        .option("subscription", full_subscription) \
        .load()


def read_pubsub_stream_alternative(spark: SparkSession, subscription: str):
    """
    Alternative method using GCS files as intermediary.
    
    This is a workaround when the native Pub/Sub connector isn't available.
    Uses Dataflow or a separate process to write Pub/Sub → GCS, then Spark reads GCS.
    """
    # Read from GCS staging area where another process writes Pub/Sub messages
    staging_path = f"gs://{GCS_BUCKET}/staging/{subscription.replace('-spark-sub', '')}/"
    
    print(f"  Reading from GCS staging: {staging_path}")
    
    return spark.readStream \
        .format("json") \
        .option("path", staging_path) \
        .option("maxFilesPerTrigger", 100) \
        .load()


def process_events(df, schema, event_type: str):
    """
    Process raw events:
    - Parse JSON
    - Add timestamp columns
    - Add partitioning columns
    """
    
    # Parse JSON from Pub/Sub message data
    parsed = df \
        .selectExpr("CAST(data AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema).alias("event")) \
        .select("event.*")
    
    # Add derived columns
    return parsed \
        .withColumn("event_timestamp", to_timestamp(col("ts") / 1000)) \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("year", year(col("event_timestamp"))) \
        .withColumn("month", month(col("event_timestamp"))) \
        .withColumn("day", dayofmonth(col("event_timestamp"))) \
        .withColumn("hour", hour(col("event_timestamp"))) \
        .filter(col("userId").isNotNull())  # Filter out invalid events


def write_to_gcs_streaming(df, output_path: str, checkpoint_name: str, trigger_interval: str):
    """
    Write streaming DataFrame to GCS with partitioning.
    
    Uses append mode with exactly-once semantics via checkpointing.
    """
    full_output_path = f"gs://{GCS_BUCKET}/{output_path}"
    checkpoint_path = f"gs://{GCS_BUCKET}/checkpoints/streaming/{checkpoint_name}"
    
    print(f"  Output: {full_output_path}")
    print(f"  Checkpoint: {checkpoint_path}")
    print(f"  Trigger: {trigger_interval}")
    
    return df.writeStream \
        .format("parquet") \
        .option("path", full_output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("year", "month", "day", "hour") \
        .trigger(processingTime=trigger_interval) \
        .outputMode("append") \
        .queryName(checkpoint_name) \
        .start()


def write_to_bigquery_streaming(df, table_name: str, checkpoint_name: str, trigger_interval: str):
    """
    Write streaming DataFrame directly to BigQuery.
    
    Requires spark-bigquery connector.
    """
    checkpoint_path = f"gs://{GCS_BUCKET}/checkpoints/streaming/{checkpoint_name}_bq"
    
    return df.writeStream \
        .format("bigquery") \
        .option("table", f"{GCP_PROJECT}.raw.{table_name}") \
        .option("checkpointLocation", checkpoint_path) \
        .option("temporaryGcsBucket", GCS_BUCKET) \
        .trigger(processingTime=trigger_interval) \
        .outputMode("append") \
        .start()


def start_event_stream(spark: SparkSession, event_type: str, config: dict, trigger_interval: str):
    """Start a streaming query for a single event type"""
    
    print(f"\nStarting stream: {event_type}")
    
    try:
        # Read from Pub/Sub
        raw_df = read_pubsub_stream(spark, config["subscription"])
        
        # Process events
        processed_df = process_events(raw_df, config["schema"], event_type)
        
        # Write to GCS
        query = write_to_gcs_streaming(
            processed_df,
            config["output_path"],
            event_type,
            trigger_interval
        )
        
        print(f"  ✓ Stream started: {event_type}")
        return query
        
    except Exception as e:
        print(f"  ✗ Failed to start {event_type}: {e}")
        return None


def run_continuous_streaming(spark: SparkSession, trigger_interval: str):
    """
    Run continuous streaming for all event types.
    
    This is the main production mode that processes events
    as they arrive with minimal latency.
    """
    print("\n" + "=" * 70)
    print("CONTINUOUS STREAMING MODE")
    print(f"Trigger Interval: {trigger_interval}")
    print("=" * 70)
    
    shutdown = GracefulShutdown()
    queries = []
    
    # Start all streams
    for event_type, config in EVENT_CONFIGS.items():
        query = start_event_stream(spark, event_type, config, trigger_interval)
        if query:
            queries.append(query)
    
    if not queries:
        print("\nERROR: No streaming queries started!")
        sys.exit(1)
    
    print(f"\n{'=' * 70}")
    print(f"STREAMING ACTIVE: {len(queries)} queries running")
    print(f"Press Ctrl+C to stop gracefully")
    print(f"{'=' * 70}\n")
    
    # Monitor queries
    while not shutdown.shutdown_requested:
        try:
            # Check query status every 30 seconds
            for query in queries:
                if not query.isActive:
                    print(f"WARNING: Query {query.name} stopped!")
                    
            # Wait for any termination
            spark.streams.awaitAnyTermination(timeout=30000)
            
        except Exception as e:
            if shutdown.shutdown_requested:
                break
            print(f"Error in monitoring loop: {e}")
    
    # Graceful shutdown
    print("\nStopping all queries...")
    for query in queries:
        try:
            query.stop()
            print(f"  Stopped: {query.name}")
        except Exception as e:
            print(f"  Error stopping {query.name}: {e}")
    
    print("All queries stopped. Exiting.")


def run_single_event_stream(spark: SparkSession, event_type: str, trigger_interval: str):
    """Run streaming for a single event type (useful for testing)"""
    
    if event_type not in EVENT_CONFIGS:
        print(f"ERROR: Unknown event type: {event_type}")
        print(f"Available: {list(EVENT_CONFIGS.keys())}")
        sys.exit(1)
    
    config = EVENT_CONFIGS[event_type]
    query = start_event_stream(spark, event_type, config, trigger_interval)
    
    if query:
        print(f"\nStreaming {event_type}. Press Ctrl+C to stop.")
        query.awaitTermination()
    else:
        sys.exit(1)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="SoundFlow Real-time Streaming Pipeline"
    )
    
    parser.add_argument(
        "--mode",
        choices=["continuous", "micro-batch", "batch", "single"],
        default="micro-batch",
        help="Processing mode"
    )
    
    parser.add_argument(
        "--event-type",
        choices=list(EVENT_CONFIGS.keys()),
        help="Event type for single mode"
    )
    
    parser.add_argument(
        "--trigger",
        default=None,
        help="Custom trigger interval (e.g., '30 seconds')"
    )
    
    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_args()
    
    # Determine trigger interval based on mode
    if args.trigger:
        trigger_interval = args.trigger
    elif args.mode == "continuous":
        trigger_interval = TRIGGER_INTERVAL_CONTINUOUS
    elif args.mode == "batch":
        trigger_interval = TRIGGER_INTERVAL_BATCH
    else:
        trigger_interval = TRIGGER_INTERVAL_MICRO_BATCH
    
    print("\n" + "=" * 70)
    print("SoundFlow Real-time Streaming Pipeline")
    print("=" * 70)
    print(f"Mode: {args.mode}")
    print(f"GCP Project: {GCP_PROJECT}")
    print(f"GCS Bucket: gs://{GCS_BUCKET}")
    print(f"Trigger Interval: {trigger_interval}")
    print("=" * 70 + "\n")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        if args.mode == "single":
            if not args.event_type:
                print("ERROR: --event-type required for single mode")
                sys.exit(1)
            run_single_event_stream(spark, args.event_type, trigger_interval)
        else:
            run_continuous_streaming(spark, trigger_interval)
            
    finally:
        spark.stop()
        print("Spark session stopped.")


if __name__ == "__main__":
    main()
