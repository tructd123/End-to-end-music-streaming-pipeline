"""
SoundFlow AI Chatbot - Agent State Definition

Defines the state schema for the LangGraph workflow.
"""

from typing import Annotated, Optional
from typing_extensions import TypedDict
from langgraph.graph.message import add_messages


class AgentState(TypedDict):
    """State for the SoundFlow AI Agent.

    Attributes:
        messages: Conversation history (managed by LangGraph's add_messages reducer).
        user_id: Optional user ID for personalized queries.
        conversation_id: Optional session ID for conversation memory.
    """

    messages: Annotated[list, add_messages]
    user_id: str | None
    conversation_id: Optional[str]
