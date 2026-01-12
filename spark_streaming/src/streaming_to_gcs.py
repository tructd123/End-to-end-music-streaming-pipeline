"""
Spark Streaming Application
Consumes events from Redpanda and writes to GCS every 2 minutes
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, year, month, dayofmonth, hour,
    current_timestamp, window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType
)

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
GCS_BUCKET = os.getenv("GCS_BUCKET", "your-bucket-name")
GCP_PROJECT = os.getenv("GCP_PROJECT", "your-project-id")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/checkpoints")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "2 minutes")

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


def create_spark_session():
    """Create Spark session with GCS configuration"""
    spark = SparkSession.builder \
        .appName("MusicStreamingPipeline") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.17") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", 
                "/opt/spark/credentials/gcp-key.json") \
        .config("spark.hadoop.fs.gs.impl", 
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", 
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .getOrCreate()
    
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
        .load()


def process_listen_events(df):
    """Process listen events with schema parsing"""
    return df \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), LISTEN_EVENT_SCHEMA).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", to_timestamp(col("ts") / 1000)) \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("year", year(col("event_timestamp"))) \
        .withColumn("month", month(col("event_timestamp"))) \
        .withColumn("day", dayofmonth(col("event_timestamp"))) \
        .withColumn("hour", hour(col("event_timestamp")))


def process_page_view_events(df):
    """Process page view events with schema parsing"""
    return df \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), PAGE_VIEW_EVENT_SCHEMA).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", to_timestamp(col("ts") / 1000)) \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("year", year(col("event_timestamp"))) \
        .withColumn("month", month(col("event_timestamp"))) \
        .withColumn("day", dayofmonth(col("event_timestamp"))) \
        .withColumn("hour", hour(col("event_timestamp")))


def process_auth_events(df):
    """Process auth events with schema parsing"""
    return df \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), AUTH_EVENT_SCHEMA).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", to_timestamp(col("ts") / 1000)) \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("year", year(col("event_timestamp"))) \
        .withColumn("month", month(col("event_timestamp"))) \
        .withColumn("day", dayofmonth(col("event_timestamp"))) \
        .withColumn("hour", hour(col("event_timestamp")))


def write_to_gcs(df, topic_name, checkpoint_suffix):
    """Write streaming dataframe to GCS in Parquet format with partitioning"""
    output_path = f"gs://{GCS_BUCKET}/raw/{topic_name}"
    checkpoint_path = f"{CHECKPOINT_LOCATION}/{checkpoint_suffix}"
    
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
    print("=" * 60)
    print("Starting Music Streaming Pipeline")
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"GCS Bucket: {GCS_BUCKET}")
    print(f"Trigger Interval: {TRIGGER_INTERVAL}")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Read from Kafka topics
    listen_df = read_kafka_stream(spark, "listen_events")
    page_view_df = read_kafka_stream(spark, "page_view_events")
    auth_df = read_kafka_stream(spark, "auth_events")
    
    # Process events
    processed_listen = process_listen_events(listen_df)
    processed_page_view = process_page_view_events(page_view_df)
    processed_auth = process_auth_events(auth_df)
    
    # Write to GCS
    listen_query = write_to_gcs(processed_listen, "listen_events", "listen")
    page_view_query = write_to_gcs(processed_page_view, "page_view_events", "page_view")
    auth_query = write_to_gcs(processed_auth, "auth_events", "auth")
    
    print("Streaming queries started. Waiting for termination...")
    
    # Wait for all queries
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
