"""
Publish test events to Pub/Sub topics
"""
from google.cloud import pubsub_v1
import json
import time

PROJECT_ID = "graphic-boulder-483814-g7"

def publish_events():
    client = pubsub_v1.PublisherClient()
    
    # Listen events
    listen_topic = client.topic_path(PROJECT_ID, 'listen-events')
    listen_events = [
        {"ts":1704067500000,"userId":"4","sessionId":103,"page":"NextSong","auth":"Logged In","method":"PUT","status":200,"level":"free","itemInSession":1,"location":"Chicago IL","userAgent":"Firefox","lastName":"Wilson","firstName":"Emma","gender":"F","registration":1700000000000,"artist":"Adele","song":"Hello","duration":295.0},
        {"ts":1704067600000,"userId":"5","sessionId":104,"page":"NextSong","auth":"Logged In","method":"PUT","status":200,"level":"paid","itemInSession":2,"location":"Seattle WA","userAgent":"Chrome","lastName":"Taylor","firstName":"Alex","gender":"M","registration":1699000000000,"artist":"Ed Sheeran","song":"Shape of You","duration":234.5},
        {"ts":1704067700000,"userId":"6","sessionId":105,"page":"NextSong","auth":"Logged In","method":"PUT","status":200,"level":"free","itemInSession":3,"location":"Boston MA","userAgent":"Safari","lastName":"Anderson","firstName":"Olivia","gender":"F","registration":1698000000000,"artist":"Taylor Swift","song":"Blank Space","duration":231.0},
    ]
    
    print("Publishing listen events...")
    for event in listen_events:
        data = json.dumps(event).encode('utf-8')
        future = client.publish(listen_topic, data)
        print(f"  Published listen event: {future.result()}")
    
    # Page view events
    page_topic = client.topic_path(PROJECT_ID, 'page-view-events')
    page_events = [
        {"ts":1704067500000,"userId":"4","sessionId":103,"page":"Home","auth":"Logged In","method":"GET","status":200,"level":"free","itemInSession":0,"location":"Chicago IL","userAgent":"Firefox","lastName":"Wilson","firstName":"Emma","gender":"F","registration":1700000000000},
        {"ts":1704067550000,"userId":"4","sessionId":103,"page":"Settings","auth":"Logged In","method":"GET","status":200,"level":"free","itemInSession":1,"location":"Chicago IL","userAgent":"Firefox","lastName":"Wilson","firstName":"Emma","gender":"F","registration":1700000000000},
    ]
    
    print("Publishing page view events...")
    for event in page_events:
        data = json.dumps(event).encode('utf-8')
        future = client.publish(page_topic, data)
        print(f"  Published page view event: {future.result()}")
    
    # Auth events
    auth_topic = client.topic_path(PROJECT_ID, 'auth-events')
    auth_events = [
        {"ts":1704067400000,"userId":"4","sessionId":103,"page":"Login","auth":"","method":"POST","status":200,"level":"free","firstName":"Emma","lastName":"Wilson","gender":"F","registration":1700000000000,"location":"Chicago IL","userAgent":"Firefox","success":"true"},
        {"ts":1704067450000,"userId":"7","sessionId":106,"page":"Login","auth":"","method":"POST","status":401,"level":"","firstName":"Failed","lastName":"User","gender":"M","registration":0,"location":"Unknown","userAgent":"Bot","success":"false"},
    ]
    
    print("Publishing auth events...")
    for event in auth_events:
        data = json.dumps(event).encode('utf-8')
        future = client.publish(auth_topic, data)
        print(f"  Published auth event: {future.result()}")
    
    # Status change events  
    status_topic = client.topic_path(PROJECT_ID, 'status-change-events')
    status_events = [
        {"ts":1704067800000,"userId":"5","sessionId":104,"auth":"Logged In","level":"paid","firstName":"Alex","lastName":"Taylor","gender":"M","registration":1699000000000,"location":"Seattle WA","userAgent":"Chrome"},
    ]
    
    print("Publishing status change events...")
    for event in status_events:
        data = json.dumps(event).encode('utf-8')
        future = client.publish(status_topic, data)
        print(f"  Published status change event: {future.result()}")
    
    print("\nAll events published successfully!")

if __name__ == "__main__":
    publish_events()
