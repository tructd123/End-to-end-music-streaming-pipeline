"""
Spark Streaming Test Script (Local)
Consumes events from Redpanda and prints to console
NO GCS required - just for testing
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, year, month, dayofmonth, hour,
    current_timestamp, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType
)

# Configuration - Use redpanda:29092 for Docker, localhost:9092 for Windows host
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")

# Schema cho listen events
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
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("userAgent", StringType(), True),
])


def create_spark_session():
    """Create Spark session for local testing"""
    spark = SparkSession.builder \
        .appName("MusicStreamingTest") \
        .master("local[*]") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
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
        .select(
            "event_timestamp",
            "userId", 
            "firstName",
            "lastName",
            "song",
            "artist",
            "city",
            "state",
            "level"
        )


def main():
    """Main entry point"""
    print("=" * 70)
    print("🎵 Starting Music Streaming Pipeline - LOCAL TEST")
    print(f"📡 Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print("=" * 70)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Read from listen_events topic
    listen_df = read_kafka_stream(spark, "listen_events")
    
    # Process events
    processed = process_listen_events(listen_df)
    
    # Write to console (for testing)
    query = processed.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 10) \
        .trigger(processingTime="10 seconds") \
        .outputMode("append") \
        .start()
    
    print("\n✅ Streaming query started!")
    print("📊 Showing listen events every 10 seconds...")
    print("⏹️  Press Ctrl+C to stop\n")
    
    query.awaitTermination()


if __name__ == "__main__":
    main()
