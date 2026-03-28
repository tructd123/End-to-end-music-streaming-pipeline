"""
SoundFlow AI Chatbot - FastAPI Server

Provides REST API endpoints for the chatbot.

Usage:
    uvicorn app:app --reload --port 8000
"""

import sys

# Fix Windows encoding for emoji/Vietnamese characters
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from agent.graph import graph
from agent.response_format import normalize_response_text
from config import settings
from memory.store import ConversationStore

# Global conversation store instance
conversation_store = ConversationStore(
    ttl_seconds=settings.MEMORY_TTL_SECONDS,
    max_sessions=settings.MEMORY_MAX_SESSIONS,
)


# ---------------------------------------------------------------------------
# Lifespan (startup / shutdown)
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Validate settings on startup."""
    missing = settings.validate()
    if missing:
        print(f"⚠️  Missing env vars: {', '.join(missing)}")
        print("   Some features may not work. Set them in .env")
    else:
        print("✅ All required settings configured!")

    print(f"🚀 SoundFlow Chatbot API starting on {settings.API_HOST}:{settings.API_PORT}")
    print(f"💾 Memory: TTL={settings.MEMORY_TTL_SECONDS}s, max={settings.MEMORY_MAX_SESSIONS} sessions")
    yield
    print("👋 Shutting down...")


# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="SoundFlow AI Chatbot",
    description=(
        "AI Agent Chatbot cho SoundFlow - tư vấn bài hát, "
        "quản lý subscription, và thống kê người dùng."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# Mount static files (CSS, JS)
_static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=_static_dir), name="static")

# CORS - allow frontend connections
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------
class ChatRequest(BaseModel):
    """Chat request payload."""

    message: str
    user_id: str | None = None
    conversation_id: str | None = None


class ChatResponse(BaseModel):
    """Chat response payload."""

    response: str
    user_id: str | None = None
    conversation_id: str | None = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    """Handle favicon requests to prevent 404 errors."""
    from fastapi import Response
    return Response(status_code=204)

@app.get("/")
async def serve_chat_ui():
    """Serve the frontend chat UI."""
    return FileResponse(os.path.join(_static_dir, "index.html"))


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "soundflow-chatbot"}


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Main chat endpoint.

    Sends the user message through the LangGraph agent and returns the response.
    """
    if not request.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    try:
        from langchain_core.messages import HumanMessage

        # Load or create conversation session
        conv_id, history = conversation_store.get_or_create(
            request.conversation_id
        )

        # Build state with conversation history + new message
        initial_state = {
            "messages": history + [HumanMessage(content=request.message)],
            "user_id": request.user_id,
            "conversation_id": conv_id,
        }

        # Run the agent graph
        result = await graph.ainvoke(initial_state)

        # Save updated message history to store
        conversation_store.save(conv_id, result["messages"])

        # Extract the last AI message
        ai_message = result["messages"][-1]
        response_content = ai_message.content

        # Handle Gemini's content format: can be str or list of parts
        if isinstance(response_content, list):
            text_parts = []
            for part in response_content:
                if isinstance(part, dict) and "text" in part:
                    text_parts.append(part["text"])
                elif isinstance(part, str):
                    text_parts.append(part)
            response_text = "\n".join(text_parts)
        else:
            response_text = str(response_content)

        response_text = normalize_response_text(response_text)

        return ChatResponse(
            response=response_text,
            user_id=request.user_id,
            conversation_id=conv_id,
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing message: {str(e)}",
        )


@app.get("/chat/stream")
async def chat_stream(
    message: str,
    user_id: str | None = None,
    conversation_id: str | None = None,
):
    """SSE streaming endpoint.

    Streams agent response tokens in real-time via Server-Sent Events.

    Query params:
        message: User message text.
        user_id: Optional user ID for personalized queries.
        conversation_id: Optional session ID for conversation memory.
    """
    if not message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    from langchain_core.messages import HumanMessage
    from sse_starlette.sse import EventSourceResponse
    from agent.streaming import stream_agent_response

    # Load or create conversation session
    conv_id, history = conversation_store.get_or_create(conversation_id)

    # Build messages with history + new message
    messages = history + [HumanMessage(content=message)]

    async def event_generator():
        collected_tokens = []
        async for event_data in stream_agent_response(
            messages=messages,
            user_id=user_id,
            conversation_id=conv_id,
        ):
            # Collect tokens for saving to memory later
            import json as _json
            try:
                parsed = _json.loads(event_data)
                if parsed.get("type") == "token":
                    collected_tokens.append(parsed.get("content", ""))
            except (ValueError, KeyError):
                pass
            yield event_data

        # Save conversation with the full response to memory
        from langchain_core.messages import AIMessage
        full_response = "".join(collected_tokens)
        if full_response:
            updated_messages = messages + [
                AIMessage(content=full_response)
            ]
            conversation_store.save(conv_id, updated_messages)

    return EventSourceResponse(event_generator())


# ---------------------------------------------------------------------------
# Run with: uvicorn app:app --reload
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=True,
    )
