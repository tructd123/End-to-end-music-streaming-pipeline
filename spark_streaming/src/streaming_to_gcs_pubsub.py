"""
Spark Streaming Application for GCP
Consumes events from Pub/Sub and writes to GCS in Parquet format
Supports both Kafka (local) and Pub/Sub (GCP) sources
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, year, month, dayofmonth, hour,
    current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType
)

# Configuration from environment variables
SOURCE_TYPE = os.getenv("SOURCE_TYPE", "kafka")  # "kafka" or "pubsub"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
GCP_PROJECT = os.getenv("GCP_PROJECT", "your-project-id")
GCS_BUCKET = os.getenv("GCS_BUCKET", "your-bucket-name")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "gs://{}/checkpoints/spark".format(os.getenv("GCS_BUCKET", "your-bucket-name")))
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "2 minutes")
PUBSUB_SUBSCRIPTION_PREFIX = os.getenv("PUBSUB_SUBSCRIPTION_PREFIX", "spark")

# Schema definitions for different event types
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
    StructField("lng", DoubleType(), True),
    StructField("lat", DoubleType(), True),
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
    StructField("lng", DoubleType(), True),
    StructField("lat", DoubleType(), True),
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
    StructField("success", StringType(), True),
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


def create_spark_session():
    """Create Spark session with GCS and Pub/Sub configuration"""
    builder = SparkSession.builder \
        .appName("SoundFlowStreamingPipeline-GCP")
    
    # GCS connector packages
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.17",
    ]
    
    # Add Pub/Sub connector if using Pub/Sub
    if SOURCE_TYPE == "pubsub":
        packages.append("com.google.cloud.spark:spark-pubsub:1.0.0-beta")
    
    builder = builder \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/spark/credentials/gcp-key.json")) \
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark, topic):
    """Read streaming data from Kafka/Redpanda topic"""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("kafka.group.id", f"soundflow-spark-{topic}") \
        .load()


def read_pubsub_stream(spark, subscription):
    """Read streaming data from Pub/Sub subscription"""
    full_subscription = f"projects/{GCP_PROJECT}/subscriptions/{subscription}"
    
    return spark.readStream \
        .format("pubsub") \
        .option("subscription", full_subscription) \
        .load()


def read_stream(spark, topic_name):
    """Read stream from configured source (Kafka or Pub/Sub)"""
    if SOURCE_TYPE == "pubsub":
        # Pub/Sub subscription name format: spark-{topic}-sub
        subscription = f"{PUBSUB_SUBSCRIPTION_PREFIX}-{topic_name.replace('_', '-')}-sub"
        return read_pubsub_stream(spark, subscription)
    else:
        return read_kafka_stream(spark, topic_name)


def process_events(df, schema, source_type="kafka"):
    """Process events with schema parsing"""
    # Different data format for Kafka vs Pub/Sub
    if source_type == "pubsub":
        # Pub/Sub returns 'data' column as bytes
        parsed = df \
            .selectExpr("CAST(data AS STRING) as json_value") \
            .select(from_json(col("json_value"), schema).alias("data")) \
            .select("data.*")
    else:
        # Kafka returns 'value' column
        parsed = df \
            .selectExpr("CAST(value AS STRING) as json_value") \
            .select(from_json(col("json_value"), schema).alias("data")) \
            .select("data.*")
    
    return parsed \
        .withColumn("event_timestamp", to_timestamp(col("ts") / 1000)) \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("year", year(col("event_timestamp"))) \
        .withColumn("month", month(col("event_timestamp"))) \
        .withColumn("day", dayofmonth(col("event_timestamp"))) \
        .withColumn("hour", hour(col("event_timestamp")))


def write_to_gcs(df, topic_name, checkpoint_suffix):
    """Write streaming dataframe to GCS in Parquet format with partitioning"""
    output_path = f"gs://{GCS_BUCKET}/raw/{topic_name}"
    checkpoint_path = f"gs://{GCS_BUCKET}/checkpoints/spark/{checkpoint_suffix}"
    
    return df.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("year", "month", "day", "hour") \
        .trigger(processingTime=TRIGGER_INTERVAL) \
        .outputMode("append") \
        .start()


def main():
    """Main entry point"""
    print("=" * 70)
    print("Starting SoundFlow Streaming Pipeline (GCP)")
    print(f"Source Type: {SOURCE_TYPE}")
    if SOURCE_TYPE == "kafka":
        print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    else:
        print(f"GCP Project: {GCP_PROJECT}")
    print(f"GCS Bucket: gs://{GCS_BUCKET}")
    print(f"Checkpoint Location: {CHECKPOINT_LOCATION}")
    print(f"Trigger Interval: {TRIGGER_INTERVAL}")
    print("=" * 70)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Event topic configurations
    events_config = [
        ("listen_events", LISTEN_EVENT_SCHEMA, "listen"),
        ("page_view_events", PAGE_VIEW_EVENT_SCHEMA, "page_view"),
        ("auth_events", AUTH_EVENT_SCHEMA, "auth"),
        ("status_change_events", STATUS_CHANGE_SCHEMA, "status_change"),
    ]
    
    queries = []
    
    for topic_name, schema, checkpoint_suffix in events_config:
        try:
            print(f"Setting up stream for: {topic_name}")
            
            # Read stream
            raw_df = read_stream(spark, topic_name)
            
            # Process events
            processed_df = process_events(raw_df, schema, SOURCE_TYPE)
            
            # Write to GCS
            query = write_to_gcs(processed_df, topic_name, checkpoint_suffix)
            queries.append(query)
            
            print(f"  ✓ Stream started for {topic_name}")
        except Exception as e:
            print(f"  ✗ Failed to start stream for {topic_name}: {e}")
    
    if not queries:
        print("No streaming queries started. Exiting...")
        sys.exit(1)
    
    print(f"\n{len(queries)} streaming queries started. Waiting for termination...")
    
    # Wait for all queries
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
