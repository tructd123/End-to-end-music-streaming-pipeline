"""
Publish test events using gcloud with base64 encoding
"""
import subprocess
import json
import base64

PROJECT_ID = "graphic-boulder-483814-g7"

def publish_event(topic, event_data):
    """Publish event to Pub/Sub topic using gcloud"""
    json_str = json.dumps(event_data)
    
    cmd = [
        "gcloud", "pubsub", "topics", "publish", topic,
        f"--project={PROJECT_ID}",
        f"--message={json_str}"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
    if result.returncode == 0:
        print(f"  ✓ Published to {topic}")
        return True
    else:
        print(f"  ✗ Failed: {result.stderr}")
        return False

def main():
    print("Publishing test events to SoundFlow...")
    print("=" * 60)
    
    # Listen events
    listen_events = [
        {"ts":1704067500000,"userId":"4","sessionId":103,"page":"NextSong","auth":"LoggedIn","method":"PUT","status":200,"level":"free","itemInSession":1,"location":"Chicago","userAgent":"Firefox","lastName":"Wilson","firstName":"Emma","gender":"F","registration":1700000000000,"artist":"Adele","song":"Hello","duration":295.0},
        {"ts":1704067600000,"userId":"5","sessionId":104,"page":"NextSong","auth":"LoggedIn","method":"PUT","status":200,"level":"paid","itemInSession":2,"location":"Seattle","userAgent":"Chrome","lastName":"Taylor","firstName":"Alex","gender":"M","registration":1699000000000,"artist":"EdSheeran","song":"ShapeOfYou","duration":234.5},
        {"ts":1704067700000,"userId":"6","sessionId":105,"page":"NextSong","auth":"LoggedIn","method":"PUT","status":200,"level":"free","itemInSession":3,"location":"Boston","userAgent":"Safari","lastName":"Anderson","firstName":"Olivia","gender":"F","registration":1698000000000,"artist":"TaylorSwift","song":"BlankSpace","duration":231.0},
    ]
    
    print("Publishing listen events...")
    for event in listen_events:
        publish_event("listen-events", event)
    
    # Page view events
    page_events = [
        {"ts":1704067500000,"userId":"4","sessionId":103,"page":"Home","auth":"LoggedIn","method":"GET","status":200,"level":"free","itemInSession":0,"location":"Chicago","userAgent":"Firefox","lastName":"Wilson","firstName":"Emma","gender":"F","registration":1700000000000},
        {"ts":1704067550000,"userId":"4","sessionId":103,"page":"Settings","auth":"LoggedIn","method":"GET","status":200,"level":"free","itemInSession":1,"location":"Chicago","userAgent":"Firefox","lastName":"Wilson","firstName":"Emma","gender":"F","registration":1700000000000},
    ]
    
    print("Publishing page view events...")
    for event in page_events:
        publish_event("page-view-events", event)
    
    # Auth events
    auth_events = [
        {"ts":1704067400000,"userId":"4","sessionId":103,"page":"Login","auth":"","method":"POST","status":200,"level":"free","firstName":"Emma","lastName":"Wilson","gender":"F","registration":1700000000000,"location":"Chicago","userAgent":"Firefox","success":"true"},
    ]
    
    print("Publishing auth events...")
    for event in auth_events:
        publish_event("auth-events", event)
    
    # Status change events  
    status_events = [
        {"ts":1704067800000,"userId":"5","sessionId":104,"auth":"LoggedIn","level":"paid","firstName":"Alex","lastName":"Taylor","gender":"M","registration":1699000000000,"location":"Seattle","userAgent":"Chrome"},
    ]
    
    print("Publishing status change events...")
    for event in status_events:
        publish_event("status-change-events", event)
    
    print("=" * 60)
    print("All events published!")

if __name__ == "__main__":
    main()
