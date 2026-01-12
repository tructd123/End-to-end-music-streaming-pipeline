"""
SoundFlow - Streaming to PostgreSQL
Consume events from Redpanda and write to PostgreSQL
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType, DoubleType
import os

# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "soundflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "soundflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "soundflow123")

POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# ============================================================
# SCHEMAS
# ============================================================

LISTEN_EVENT_SCHEMA = StructType([
    StructField("ts", LongType(), True),
    StructField("userId", IntegerType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("auth", StringType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("registration", LongType(), True),
    StructField("gender", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("song", StringType(), True),
    StructField("duration", DoubleType(), True),
])

PAGE_VIEW_EVENT_SCHEMA = StructType([
    StructField("ts", LongType(), True),
    StructField("userId", IntegerType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("page", StringType(), True),
    StructField("auth", StringType(), True),
    StructField("method", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("registration", LongType(), True),
    StructField("gender", StringType(), True),
])

AUTH_EVENT_SCHEMA = StructType([
    StructField("ts", LongType(), True),
    StructField("userId", IntegerType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("registration", LongType(), True),
    StructField("gender", StringType(), True),
    StructField("success", BooleanType(), True),
])

STATUS_CHANGE_EVENT_SCHEMA = StructType([
    StructField("ts", LongType(), True),
    StructField("userId", IntegerType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("level", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("registration", LongType(), True),
    StructField("gender", StringType(), True),
])

# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():
    """Create Spark session with PostgreSQL connector"""
    return SparkSession.builder \
        .appName("SoundFlow-Streaming-to-PostgreSQL") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.streaming.checkpointLocation", "/app/checkpoints") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

# ============================================================
# KAFKA STREAM READER
# ============================================================

def read_kafka_stream(spark, topic):
    """Read streaming data from Kafka topic"""
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

# ============================================================
# EVENT PROCESSORS
# ============================================================

def process_listen_events(df):
    """Process listen events"""
    parsed = df.select(
        from_json(col("value").cast("string"), LISTEN_EVENT_SCHEMA).alias("data")
    ).select("data.*")
    
    return parsed.select(
        (col("ts") / 1000).cast("timestamp").alias("event_timestamp"),
        col("userId").alias("user_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        col("song"),
        col("artist"),
        col("city"),
        col("state"),
        col("level"),
        col("sessionId").alias("session_id"),
        col("userAgent").alias("user_agent")
    ).filter(col("song").isNotNull())

def process_page_view_events(df):
    """Process page view events"""
    parsed = df.select(
        from_json(col("value").cast("string"), PAGE_VIEW_EVENT_SCHEMA).alias("data")
    ).select("data.*")
    
    return parsed.select(
        (col("ts") / 1000).cast("timestamp").alias("event_timestamp"),
        col("userId").alias("user_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        col("page"),
        col("city"),
        col("state"),
        col("level"),
        col("sessionId").alias("session_id"),
        col("userAgent").alias("user_agent")
    )

def process_auth_events(df):
    """Process authentication events"""
    parsed = df.select(
        from_json(col("value").cast("string"), AUTH_EVENT_SCHEMA).alias("data")
    ).select("data.*")
    
    return parsed.select(
        (col("ts") / 1000).cast("timestamp").alias("event_timestamp"),
        col("userId").alias("user_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        col("city"),
        col("state"),
        col("level"),
        col("sessionId").alias("session_id"),
        col("success")
    )

def process_status_change_events(df):
    """Process status change events"""
    parsed = df.select(
        from_json(col("value").cast("string"), STATUS_CHANGE_EVENT_SCHEMA).alias("data")
    ).select("data.*")
    
    return parsed.select(
        (col("ts") / 1000).cast("timestamp").alias("event_timestamp"),
        col("userId").alias("user_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        col("level").alias("new_level"),
        col("city"),
        col("state")
    )

# ============================================================
# POSTGRES WRITER
# ============================================================

def write_to_postgres(df, epoch_id, table_name):
    """Write DataFrame to PostgreSQL"""
    df.write \
        .jdbc(url=POSTGRES_JDBC_URL, 
              table=f"raw.{table_name}", 
              mode="append", 
              properties=POSTGRES_PROPERTIES)

# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 70)
    print("🎵 SoundFlow - Streaming to PostgreSQL")
    print("=" * 70)
    print(f"📡 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"🐘 PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    print("=" * 70)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Read streams from Kafka
    listen_stream = read_kafka_stream(spark, "listen_events")
    page_view_stream = read_kafka_stream(spark, "page_view_events")
    auth_stream = read_kafka_stream(spark, "auth_events")
    status_change_stream = read_kafka_stream(spark, "status_change_events")
    
    # Process events
    listen_events = process_listen_events(listen_stream)
    page_view_events = process_page_view_events(page_view_stream)
    auth_events = process_auth_events(auth_stream)
    status_change_events = process_status_change_events(status_change_stream)
    
    # Write to PostgreSQL with micro-batch (every 30 seconds)
    query_listen = listen_events.writeStream \
        .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "listen_events")) \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    query_page_view = page_view_events.writeStream \
        .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "page_view_events")) \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    query_auth = auth_events.writeStream \
        .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "auth_events")) \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    query_status = status_change_events.writeStream \
        .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "status_change_events")) \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    print("✅ All streaming queries started!")
    print("📊 Writing to PostgreSQL every 30 seconds...")
    print("⏹️  Press Ctrl+C to stop")
    print()
    
    # Wait for all queries
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
