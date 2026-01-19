"""
Kafka to GCS - Python (Non-Spark)
Simple script to consume from Kafka and upload to GCS as Parquet
"""

import os
import json
from datetime import datetime
from typing import Dict, List
from confluent_kafka import Consumer, KafkaError, KafkaException
from google.cloud import storage
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GCS_BUCKET = os.getenv("GCS_BUCKET", "tf-state-soundflow-123")
GCP_PROJECT = os.getenv("GCP_PROJECT", "graphic-boulder-483814-g7")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", 
                             "E:/Individual/Data_streaming_pipeline/credentials/dbt-sa-key.json")

# Set credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH


def create_consumer(group_id: str = "kafka-to-gcs-batch"):
    """Create Kafka consumer"""
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }
    return Consumer(conf)


def consume_all_messages(consumer: Consumer, topic: str, timeout: float = 5.0) -> List[Dict]:
    """Consume all messages from a topic"""
    print(f"\n  Subscribing to topic: {topic}")
    consumer.subscribe([topic])
    
    messages = []
    no_message_count = 0
    max_no_message = 3  # Stop after 3 consecutive empty polls
    
    while True:
        msg = consumer.poll(timeout=timeout)
        
        if msg is None:
            no_message_count += 1
            if no_message_count >= max_no_message:
                print(f"    No more messages (polled {max_no_message} times)")
                break
            continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"    Reached end of partition")
                break
            else:
                raise KafkaException(msg.error())
        
        no_message_count = 0  # Reset counter on successful message
        try:
            data = json.loads(msg.value().decode('utf-8'))
            messages.append(data)
            
            if len(messages) % 5000 == 0:
                print(f"    Consumed {len(messages)} messages...")
        except json.JSONDecodeError:
            pass
    
    consumer.unsubscribe()
    print(f"    Total messages consumed: {len(messages)}")
    return messages


def upload_to_gcs(df: pd.DataFrame, bucket_name: str, blob_path: str):
    """Upload DataFrame as Parquet to GCS with TIMESTAMP_MILLIS for BigQuery compatibility"""
    # Make a copy to avoid modifying original
    df = df.copy()
    
    # Convert timestamp columns to millisecond precision
    timestamp_cols = df.select_dtypes(include=['datetime64[ns]']).columns
    for col in timestamp_cols:
        df[col] = df[col].astype('datetime64[ms]')
    
    # Convert nullable numeric columns to string to avoid BigQuery ConvertedType issues
    # userId, sessionId can be nullable - convert to string
    for col in ['userId', 'sessionId', 'registration']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(int(x)) if pd.notna(x) else None)
    
    # Convert to PyArrow table with explicit timestamp type
    table = pa.Table.from_pandas(df)
    
    # Create GCS client
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    # Write to buffer and upload
    import io
    buffer = io.BytesIO()
    pq.write_table(table, buffer, coerce_timestamps='ms', allow_truncated_timestamps=True)
    buffer.seek(0)
    
    blob.upload_from_file(buffer, content_type='application/octet-stream')
    print(f"    Uploaded to gs://{bucket_name}/{blob_path}")


def process_topic(topic: str, output_path: str):
    """Process a single topic and upload to GCS"""
    print(f"\n{'='*50}")
    print(f"Processing: {topic}")
    print(f"{'='*50}")
    
    # Create unique consumer group for each topic to avoid offset conflicts
    consumer = create_consumer(group_id=f"kafka-to-gcs-{topic}-{datetime.now().strftime('%Y%m%d%H%M%S')}")
    
    try:
        messages = consume_all_messages(consumer, topic)
        
        if not messages:
            print(f"  No messages in topic {topic}")
            return 0
        
        # Convert to DataFrame
        df = pd.DataFrame(messages)
        print(f"  DataFrame shape: {df.shape}")
        print(f"  Columns: {list(df.columns)}")
        
        # Add timestamp column if 'ts' exists
        if 'ts' in df.columns:
            df['event_timestamp'] = pd.to_datetime(df['ts'], unit='ms')
            df['year'] = df['event_timestamp'].dt.year
            df['month'] = df['event_timestamp'].dt.month
            df['day'] = df['event_timestamp'].dt.day
            df['hour'] = df['event_timestamp'].dt.hour
        
        df['processed_at'] = datetime.now()
        
        # Show sample
        print(f"\n  Sample data:")
        print(df.head(3).to_string())
        
        # Group by partition columns and upload
        if 'year' in df.columns:
            groups = df.groupby(['year', 'month', 'day', 'hour'])
            for (year, month, day, hour), group_df in groups:
                blob_path = f"raw/{output_path}/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/data.parquet"
                upload_to_gcs(group_df, GCS_BUCKET, blob_path)
        else:
            # Single file upload
            blob_path = f"raw/{output_path}/data.parquet"
            upload_to_gcs(df, GCS_BUCKET, blob_path)
        
        print(f"  ✓ Processed {len(messages)} records for {topic}")
        return len(messages)
        
    finally:
        consumer.close()


def main():
    """Main entry point"""
    print("="*60)
    print("Kafka to GCS - Python Batch Processing")
    print("="*60)
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"GCS Bucket: {GCS_BUCKET}")
    print(f"Credentials: {CREDENTIALS_PATH}")
    
    topics = [
        ("listen_events", "listen_events"),
        ("page_view_events", "page_view_events"),
        ("auth_events", "auth_events"),
        ("status_change_events", "status_change_events"),
    ]
    
    total_records = 0
    results = []
    
    for topic, output_path in topics:
        try:
            count = process_topic(topic, output_path)
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


if __name__ == "__main__":
    main()
