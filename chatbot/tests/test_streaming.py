"""
Tests for SSE streaming endpoint and streaming module.

Tests:
  - SSE event JSON format
  - Streaming endpoint exists and accepts params
  - Conversation ID propagation
  - Error handling for empty messages
  - Backward compatibility (POST /chat still works)
"""

import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from httpx import AsyncClient, ASGITransport

# Need to patch settings before importing app
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


@pytest.fixture
def anyio_backend():
    return "asyncio"


# ------------------------------------------------------------------ #
# Test SSE event format helper
# ------------------------------------------------------------------ #
def test_sse_event_format():
    """Verify _sse_event produces valid JSON."""
    from agent.streaming import _sse_event

    result = _sse_event({"type": "token", "content": "hello"})
    parsed = json.loads(result)
    assert parsed["type"] == "token"
    assert parsed["content"] == "hello"


def test_sse_event_unicode():
    """Verify _sse_event handles Vietnamese characters."""
    from agent.streaming import _sse_event

    result = _sse_event({"type": "token", "content": "Xin chào 🎵"})
    parsed = json.loads(result)
    assert parsed["content"] == "Xin chào 🎵"


def test_sse_event_types():
    """Verify all SSE event types are valid JSON."""
    from agent.streaming import _sse_event

    events = [
        {"type": "token", "content": "text"},
        {"type": "tool_call", "name": "search_songs", "args": {"query": "test"}},
        {"type": "tool_result", "name": "search_songs", "content": "results"},
        {"type": "done", "conversation_id": "abc-123"},
        {"type": "error", "message": "Something went wrong"},
    ]
    for event in events:
        result = _sse_event(event)
        parsed = json.loads(result)
        assert parsed["type"] == event["type"]


# ------------------------------------------------------------------ #
# Test API endpoints
# ------------------------------------------------------------------ #
@pytest.mark.asyncio
async def test_health_endpoint():
    """Health endpoint returns healthy status."""
    from app import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"


@pytest.mark.asyncio
async def test_stream_endpoint_empty_message():
    """Streaming endpoint rejects empty messages."""
    from app import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/chat/stream", params={"message": "  "})
        assert resp.status_code == 400


@pytest.mark.asyncio
async def test_stream_endpoint_missing_message():
    """Streaming endpoint requires message param."""
    from app import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/chat/stream")
        assert resp.status_code == 422  # FastAPI validation error


@pytest.mark.asyncio
async def test_chat_endpoint_empty_message():
    """POST /chat rejects empty messages."""
    from app import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/chat",
            json={"message": ""},
        )
        assert resp.status_code == 400


@pytest.mark.asyncio
async def test_chat_endpoint_returns_conversation_id():
    """POST /chat returns a conversation_id even without one in request."""
    from app import app

    # Mock the graph.invoke to avoid actual LLM calls
    mock_ai_msg = MagicMock()
    mock_ai_msg.content = "Hello! How can I help you?"

    with patch("app.graph") as mock_graph:
        mock_graph.ainvoke = AsyncMock(return_value={
            "messages": [mock_ai_msg]
        })

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/chat",
                json={"message": "hello"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert "conversation_id" in data
            assert data["conversation_id"] is not None
            assert data["response"] == "Hello! How can I help you?"


@pytest.mark.asyncio
async def test_chat_backward_compatibility():
    """POST /chat works without conversation_id (backward compatible)."""
    from app import app

    mock_ai_msg = MagicMock()
    mock_ai_msg.content = "I'm SoundFlow AI!"

    with patch("app.graph") as mock_graph:
        mock_graph.ainvoke = AsyncMock(return_value={
            "messages": [mock_ai_msg]
        })

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Request WITHOUT conversation_id — should still work
            resp = await client.post(
                "/chat",
                json={"message": "who are you?"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["response"] == "I'm SoundFlow AI!"
            # A new conversation_id is generated
            assert data["conversation_id"] is not None


@pytest.mark.asyncio
async def test_root_serves_html():
    """GET / serves the chat frontend HTML."""
    from app import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/")
        assert resp.status_code == 200
        assert "SoundFlow" in resp.text
        assert "text/html" in resp.headers.get("content-type", "")


@pytest.mark.asyncio
async def test_static_files_served():
    """Static CSS and JS files are served correctly."""
    from app import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        css_resp = await client.get("/static/styles.css")
        assert css_resp.status_code == 200
        assert "glassmorphism" in css_resp.text.lower() or "bg-primary" in css_resp.text

        js_resp = await client.get("/static/app.js")
        assert js_resp.status_code == 200
        assert "SoundFlow" in js_resp.text
