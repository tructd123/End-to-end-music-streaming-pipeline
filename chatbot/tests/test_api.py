import os
import sys

from fastapi.testclient import TestClient

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from app import app

client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "soundflow-chatbot"}


def test_missing_message():
    response = client.get("/chat/stream?message=")
    assert response.status_code == 400
    assert response.json() == {"detail": "Message cannot be empty"}
