"""
Spark Batch Job - Pull from Pub/Sub and write to GCS with partitioning
For production testing on Dataproc
"""

import os
import json
import datetime
from google.cloud import pubsub_v1
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, hour, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType

# Configuration
PROJECT_ID = os.getenv("GCP_PROJECT", "graphic-boulder-483814-g7")
BUCKET = os.getenv("GCS_BUCKET", "tf-state-soundflow-123")

# Schemas
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


def pull_messages(subscription_id, max_messages=100):
    """Pull messages from Pub/Sub subscription"""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, subscription_id)
    
    print(f"  Pulling from: {subscription_path}")
    
    try:
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": max_messages},
            timeout=30.0
        )
        
        messages = []
        ack_ids = []
        
        for received_message in response.received_messages:
            data = received_message.message.data.decode("utf-8")
            ack_ids.append(received_message.ack_id)
            try:
                messages.append(json.loads(data))
            except json.JSONDecodeError:
                print(f"    Skipping invalid JSON")
        
        if ack_ids:
            subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
            print(f"    Acknowledged {len(ack_ids)} messages, {len(messages)} valid")
        
        return messages
    except Exception as e:
        print(f"    Error: {e}")
        return []


def process_and_write(spark, event_type, subscription, schema, output_path):
    """Process events and write to GCS as Parquet with partitioning"""
    print(f"\nProcessing {event_type}...")
    
    messages = pull_messages(subscription)
    
    if not messages:
        print(f"  No messages for {event_type}")
        return 0
    
    # Create DataFrame
    df = spark.createDataFrame(messages, schema)
    
    # Add timestamp columns for partitioning
    df = df.withColumn("event_timestamp", to_timestamp(col("ts") / 1000)) \
           .withColumn("processed_at", current_timestamp()) \
           .withColumn("year", year(col("event_timestamp"))) \
           .withColumn("month", month(col("event_timestamp"))) \
           .withColumn("day", dayofmonth(col("event_timestamp"))) \
           .withColumn("hour", hour(col("event_timestamp")))
    
    # Write to GCS with partitioning
    full_path = f"gs://{BUCKET}/raw/{output_path}"
    df.write \
        .mode("append") \
        .partitionBy("year", "month", "day", "hour") \
        .parquet(full_path)
    
    print(f"  ✓ Written {df.count()} records to {full_path}")
    return df.count()


def main():
    print("=" * 70)
    print("SoundFlow Spark Batch Job - Write to GCS with Partitioning")
    print(f"Project: {PROJECT_ID}")
    print(f"Bucket: {BUCKET}")
    print("=" * 70)
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("SoundFlow-BatchToGCS") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Process each event type
    events_config = [
        ("listen_events", "listen-events-spark-sub", LISTEN_EVENT_SCHEMA, "listen_events"),
        ("page_view_events", "page-view-events-spark-sub", PAGE_VIEW_SCHEMA, "page_view_events"),
        ("auth_events", "auth-events-spark-sub", AUTH_EVENT_SCHEMA, "auth_events"),
        ("status_change_events", "status-change-events-spark-sub", STATUS_CHANGE_SCHEMA, "status_change_events"),
    ]
    
    total_records = 0
    for name, subscription, schema, output_path in events_config:
        try:
            count = process_and_write(spark, name, subscription, schema, output_path)
            total_records += count
        except Exception as e:
            print(f"  ✗ Error processing {name}: {e}")
    
    print("\n" + "=" * 70)
    print(f"Total records written: {total_records}")
    print("=" * 70)
    
    spark.stop()


if __name__ == "__main__":
    main()
