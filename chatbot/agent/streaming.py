"""
SoundFlow AI Chatbot - Streaming Module

Uses `graph.astream()` to get node-level state updates, then yields
the final AI response as word-level SSE chunks for a streaming UX.

Since Gemini's ChatGoogleGenerativeAI doesn't produce per-token
`on_chat_model_stream` events via `astream_events`, we use `astream()`
to get the complete response from each node, then split it into
small chunks for the frontend to display progressively.

Event types:
    - token: Text chunk (word or small group of words)
    - tool_call: Agent invoking a tool
    - tool_result: Tool execution result
    - done: Stream complete
    - error: Error occurred
"""

import json
import re
import traceback
from typing import AsyncGenerator, Optional

from agent.graph import graph
from agent.response_format import normalize_response_text


def _sse_event(data: dict) -> str:
    """Format a dict as an SSE data line."""
    return json.dumps(data, ensure_ascii=False)


def _chunk_text(text: str, chunk_size: int = 4) -> list[str]:
    """Split text into small chunks of words for streaming effect.

    Args:
        text: Full text to chunk.
        chunk_size: Number of words per chunk.

    Returns:
        List of text chunks.
    """
    if not text:
        return []
    chunks = []
    parts = re.split(r"(\n+)", text)
    for part in parts:
        if not part:
            continue
        # Preserve line breaks as standalone chunks.
        if part.startswith("\n"):
            chunks.append(part)
            continue

        words = part.split(" ")
        for i in range(0, len(words), chunk_size):
            chunk = " ".join(words[i : i + chunk_size])
            if i + chunk_size < len(words):
                chunk += " "
            chunks.append(chunk)

    return chunks


async def stream_agent_response(
    messages: list,
    user_id: Optional[str] = None,
    conversation_id: Optional[str] = None,
) -> AsyncGenerator[str, None]:
    """Stream agent response as SSE events.

    Uses graph.astream() which yields state deltas from each node.
    Tool calls/results are emitted in real-time. The final AI response
    is chunked into small word groups for a streaming typing effect.

    Args:
        messages: Full message history including the new user message.
                  System prompt is prepended by chatbot_node automatically.
        user_id: Optional user ID for BigQuery tools.
        conversation_id: Session ID for memory tracking.

    Yields:
        SSE-formatted JSON strings.
    """
    import asyncio

    initial_state = {
        "messages": messages,
        "user_id": user_id,
        "conversation_id": conversation_id,
    }

    try:
        async for state_update in graph.astream(initial_state, stream_mode="updates"):
            # state_update is a dict: {node_name: {state_delta}}
            for node_name, delta in state_update.items():
                if node_name == "chatbot":
                    # chatbot node returns {"messages": [AIMessage(...)]}
                    new_msgs = delta.get("messages", [])
                    for msg in new_msgs:
                        if hasattr(msg, "tool_calls") and msg.tool_calls:
                            # LLM decided to call tools — emit tool_call events
                            for tc in msg.tool_calls:
                                yield _sse_event(
                                    {
                                        "type": "tool_call",
                                        "name": tc.get("name", "unknown"),
                                        "args": tc.get("args", {}),
                                    }
                                )
                        elif hasattr(msg, "content") and msg.content:
                            # Final AI text response — chunk it for streaming
                            content = msg.content
                            # Handle Gemini list format
                            if isinstance(content, list):
                                text_parts = []
                                for part in content:
                                    if isinstance(part, dict) and "text" in part:
                                        text_parts.append(part["text"])
                                    elif isinstance(part, str):
                                        text_parts.append(part)
                                full_text = "\n".join(text_parts)
                            else:
                                full_text = str(content)

                            full_text = normalize_response_text(full_text)

                            # Yield word chunks with small delays
                            for chunk in _chunk_text(full_text):
                                yield _sse_event(
                                    {
                                        "type": "token",
                                        "content": chunk,
                                    }
                                )
                                await asyncio.sleep(0.02)

                elif node_name == "tools":
                    # Tool execution results
                    new_msgs = delta.get("messages", [])
                    for msg in new_msgs:
                        tool_name = getattr(msg, "name", "unknown")
                        output_str = str(getattr(msg, "content", ""))[:500]
                        yield _sse_event(
                            {
                                "type": "tool_result",
                                "name": tool_name,
                                "content": output_str,
                            }
                        )

        # Stream complete
        yield _sse_event({"type": "done", "conversation_id": conversation_id})

    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        traceback.print_exc()
        yield _sse_event({"type": "error", "message": error_msg})
