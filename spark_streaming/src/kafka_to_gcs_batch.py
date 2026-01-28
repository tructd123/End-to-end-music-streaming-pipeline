"""
Spark Batch Job - Read from Kafka (Redpanda) and write to GCS
For testing local Kafka → GCS pipeline
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, year, month, dayofmonth, hour,
    current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType
)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GCS_BUCKET = os.getenv("GCS_BUCKET", "tf-state-soundflow-123")
GCP_PROJECT = os.getenv("GCP_PROJECT", "graphic-boulder-483814-g7")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials/dbt-sa-key.json")

# Schemas
LISTEN_EVENT_SCHEMA = StructType([
    StructField("ts", LongType(), True),
    StructField("userId", StringType(), True),
    StructField("sessionId", LongType(), True),
    StructField("auth", StringType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("song", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", LongType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("userAgent", StringType(), True),
])

PAGE_VIEW_SCHEMA = StructType([
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
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("userAgent", StringType(), True),
])

AUTH_EVENT_SCHEMA = StructType([
    StructField("ts", LongType(), True),
    StructField("userId", StringType(), True),
    StructField("sessionId", LongType(), True),
    StructField("level", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", LongType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
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
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("userAgent", StringType(), True),
])


def create_spark_session():
    """Create Spark session with GCS configuration"""
    print(f"Creating Spark session...")
    print(f"  GCS Bucket: {GCS_BUCKET}")
    print(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Credentials: {CREDENTIALS_PATH}")
    
    spark = SparkSession.builder \
        .appName("KafkaToGCS-Batch") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.17") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", 
                CREDENTIALS_PATH) \
        .config("spark.hadoop.fs.gs.impl", 
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", 
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_topic(spark, topic):
    """Read entire topic from Kafka (batch mode)"""
    print(f"\n  Reading topic: {topic}")
    
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    count = df.count()
    print(f"    Records found: {count}")
    return df, count


def process_and_write(spark, topic, schema, output_path):
    """Process events from Kafka and write to GCS"""
    print(f"\n{'='*50}")
    print(f"Processing: {topic}")
    print(f"{'='*50}")
    
    raw_df, count = read_kafka_topic(spark, topic)
    
    if count == 0:
        print(f"  No data in topic {topic}")
        return 0
    
    # Parse JSON from Kafka value
    parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema).alias("data")) \
        .select("data.*")
    
    # Add timestamp columns for partitioning
    df = parsed_df \
        .withColumn("event_timestamp", to_timestamp(col("ts") / 1000)) \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("year", year(to_timestamp(col("ts") / 1000))) \
        .withColumn("month", month(to_timestamp(col("ts") / 1000))) \
        .withColumn("day", dayofmonth(to_timestamp(col("ts") / 1000))) \
        .withColumn("hour", hour(to_timestamp(col("ts") / 1000)))
    
    # Show sample data
    print(f"\n  Sample data:")
    df.select("event_timestamp", "userId", "sessionId").show(3, truncate=False)
    
    # Write to GCS with partitioning
    gcs_path = f"gs://{GCS_BUCKET}/raw/{output_path}"
    print(f"\n  Writing to: {gcs_path}")
    
    df.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day", "hour") \
        .parquet(gcs_path)
    
    print(f"  ✓ Written {count} records to {gcs_path}")
    return count


def main():
    """Main entry point"""
    print("="*60)
    print("Kafka to GCS Batch Processing")
    print("="*60)
    
    spark = create_spark_session()
    
    topics = [
        ("listen_events", LISTEN_EVENT_SCHEMA, "listen_events"),
        ("page_view_events", PAGE_VIEW_SCHEMA, "page_view_events"),
        ("auth_events", AUTH_EVENT_SCHEMA, "auth_events"),
        ("status_change_events", STATUS_CHANGE_SCHEMA, "status_change_events"),
    ]
    
    total_records = 0
    results = []
    
    for topic, schema, output_path in topics:
        try:
            count = process_and_write(spark, topic, schema, output_path)
            results.append((topic, count, "SUCCESS"))
            total_records += count
        except Exception as e:
            print(f"  ✗ Error processing {topic}: {e}")
            results.append((topic, 0, f"ERROR: {e}"))
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    for topic, count, status in results:
        print(f"  {topic}: {count} records - {status}")
    print(f"\n  Total: {total_records} records processed")
    print("="*60)
    
    spark.stop()


if __name__ == "__main__":
    main()
