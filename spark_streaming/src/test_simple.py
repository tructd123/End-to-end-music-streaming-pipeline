"""
Simple test script - use environment variables instead of args
"""

import os
import json
import datetime

# Configuration from environment or defaults
PROJECT_ID = os.getenv("GCP_PROJECT", "graphic-boulder-483814-g7")
BUCKET = os.getenv("GCS_BUCKET", "tf-state-soundflow-123") 
SUBSCRIPTION = os.getenv("SUBSCRIPTION", "listen-events-spark-sub")

def main():
    from google.cloud import pubsub_v1
    from google.cloud import storage
    
    print("=" * 60)
    print("SoundFlow Pub/Sub Test")
    print(f"Project: {PROJECT_ID}")
    print(f"Bucket: {BUCKET}")
    print(f"Subscription: {SUBSCRIPTION}")
    print("=" * 60)
    
    # Pull messages
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION)
    
    print(f"Pulling messages from: {subscription_path}")
    
    try:
        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": 100,
            },
            timeout=30.0
        )
        
        messages = []
        ack_ids = []
        parse_errors = 0
        
        for received_message in response.received_messages:
            message_data = received_message.message.data.decode("utf-8")
            print(f"Received: {message_data[:100]}...")
            ack_ids.append(received_message.ack_id)
            
            try:
                messages.append(json.loads(message_data))
            except json.JSONDecodeError as e:
                print(f"  -> JSON parse error (skipping): {e}")
                parse_errors += 1
        
        if ack_ids:
            subscriber.acknowledge(
                request={
                    "subscription": subscription_path,
                    "ack_ids": ack_ids,
                }
            )
            print(f"Acknowledged {len(ack_ids)} messages ({parse_errors} had JSON errors)")
        
        # Write to GCS
        if messages:
            client = storage.Client()
            bucket = client.bucket(BUCKET)
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            blob_name = f"test/pubsub_test_{timestamp}.json"
            blob = bucket.blob(blob_name)
            blob.upload_from_string(json.dumps(messages, indent=2))
            print(f"Written {len(messages)} valid messages to gs://{BUCKET}/{blob_name}")
            print(f"\nTest successful! Processed {len(messages)} messages")
        else:
            print("\nNo valid messages found in subscription")
            
    except Exception as e:
        print(f"Error: {e}")
    
    print("=" * 60)

if __name__ == "__main__":
    main()
