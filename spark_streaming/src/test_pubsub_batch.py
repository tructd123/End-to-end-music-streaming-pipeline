"""
Test script to verify Pub/Sub connectivity and read messages in batch mode
"""

import argparse
import json
from google.cloud import pubsub_v1
from google.cloud import storage

def pull_messages(project_id, subscription_id, max_messages=10):
    """Pull messages from Pub/Sub subscription"""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    
    print(f"Pulling messages from: {subscription_path}")
    
    response = subscriber.pull(
        request={
            "subscription": subscription_path,
            "max_messages": max_messages,
        },
        timeout=30.0
    )
    
    messages = []
    ack_ids = []
    
    for received_message in response.received_messages:
        message_data = received_message.message.data.decode("utf-8")
        print(f"Received: {message_data}")
        messages.append(json.loads(message_data))
        ack_ids.append(received_message.ack_id)
    
    if ack_ids:
        subscriber.acknowledge(
            request={
                "subscription": subscription_path,
                "ack_ids": ack_ids,
            }
        )
        print(f"Acknowledged {len(ack_ids)} messages")
    
    return messages

def write_to_gcs(bucket_name, blob_name, data):
    """Write data to GCS"""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(json.dumps(data, indent=2))
    print(f"Written to gs://{bucket_name}/{blob_name}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True, help='GCP Project ID')
    parser.add_argument('--bucket', required=True, help='GCS Bucket name')
    parser.add_argument('--subscription', default='listen-events-spark-sub', help='Pub/Sub subscription')
    parser.add_argument('--max-messages', type=int, default=10, help='Max messages to pull')
    args = parser.parse_args()
    
    print("=" * 60)
    print("SoundFlow Pub/Sub Test")
    print(f"Project: {args.project}")
    print(f"Bucket: {args.bucket}")
    print(f"Subscription: {args.subscription}")
    print("=" * 60)
    
    # Pull messages
    messages = pull_messages(args.project, args.subscription, args.max_messages)
    
    if messages:
        # Write to GCS
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_name = f"test/pubsub_test_{timestamp}.json"
        write_to_gcs(args.bucket, blob_name, messages)
        print(f"\nTest successful! Processed {len(messages)} messages")
    else:
        print("\nNo messages found in subscription")
    
    print("=" * 60)

if __name__ == "__main__":
    main()
