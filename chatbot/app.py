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

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from agent.graph import graph
from config import settings


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


class ChatResponse(BaseModel):
    """Chat response payload."""

    response: str
    user_id: str | None = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
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
        # Build initial state
        from langchain_core.messages import HumanMessage

        initial_state = {
            "messages": [HumanMessage(content=request.message)],
            "user_id": request.user_id,
        }

        # Run the agent graph
        result = graph.invoke(initial_state)

        # Extract the last AI message
        ai_message = result["messages"][-1]
        response_content = ai_message.content

        # Handle Gemini's content format: can be str or list of parts
        if isinstance(response_content, list):
            # Extract text from content parts like [{'type': 'text', 'text': '...'}]
            text_parts = []
            for part in response_content:
                if isinstance(part, dict) and "text" in part:
                    text_parts.append(part["text"])
                elif isinstance(part, str):
                    text_parts.append(part)
            response_text = "\n".join(text_parts)
        else:
            response_text = str(response_content)

        return ChatResponse(
            response=response_text,
            user_id=request.user_id,
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing message: {str(e)}",
        )


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
