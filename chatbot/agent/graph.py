"""
SoundFlow AI Chatbot - LangGraph Workflow

Implements the ReAct agent pattern:
  User message → LLM (chatbot_node) → Tool calls? → tools_node → LLM → Response
"""

from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import END, StateGraph
from langgraph.prebuilt import ToolNode

from agent.prompts import SOUNDFLOW_SYSTEM_PROMPT
from agent.state import AgentState
from config import settings
from tools.mood_recommender import recommend_by_mood
from tools.playlist import (
    create_playlist,
    delete_playlist,
    get_playlist,
    remove_song_from_playlist,
    update_playlist,
)
from tools.search import get_trending_songs, search_artists, search_songs, search_songs_by_artist
from tools.smart_recommender import recommend_personalized
from tools.song_recommender import recommend_songs
from tools.subscription import change_subscription
from tools.user_stats import get_user_listening_history, get_user_stats

# ---------------------------------------------------------------------------
# All tools available to the agent
# ---------------------------------------------------------------------------
ALL_TOOLS = [
    recommend_songs,
    recommend_personalized,
    recommend_by_mood,
    get_user_stats,
    get_user_listening_history,
    change_subscription,
    search_songs,
    search_artists,
    search_songs_by_artist,
    get_trending_songs,
    get_playlist,
    create_playlist,
    delete_playlist,
    update_playlist,
    remove_song_from_playlist,
]


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------
async def chatbot_node(state: AgentState) -> dict:
    """Main LLM node that processes messages and decides whether to call tools.

    Uses async `ainvoke` so that `graph.astream_events()` can produce
    `on_chat_model_stream` events for real-time token streaming.
    """

    llm = ChatGoogleGenerativeAI(
        model=settings.LLM_MODEL,
        google_api_key=settings.GOOGLE_API_KEY,
        temperature=0.7,
        streaming=True,
    )

    # Bind tools so the LLM can generate tool calls
    llm_with_tools = llm.bind_tools(ALL_TOOLS)

    # Prepend system prompt
    from langchain_core.messages import SystemMessage

    messages = state["messages"]
    if not messages or not isinstance(messages[0], SystemMessage):
        messages = [SystemMessage(content=SOUNDFLOW_SYSTEM_PROMPT)] + messages

    response = await llm_with_tools.ainvoke(messages)
    return {"messages": [response]}


def route_after_chatbot(state: AgentState) -> str:
    """Decide whether to call tools or end the conversation turn."""
    last_message = state["messages"][-1]

    # If the LLM returned tool calls, route to the tools node
    if hasattr(last_message, "tool_calls") and last_message.tool_calls:
        return "tools"
    return END


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------
def create_graph():
    """Build and compile the LangGraph agent."""

    tool_node = ToolNode(tools=ALL_TOOLS)

    graph_builder = StateGraph(AgentState)

    # Add nodes
    graph_builder.add_node("chatbot", chatbot_node)
    graph_builder.add_node("tools", tool_node)

    # Set entry point
    graph_builder.set_entry_point("chatbot")

    # Add edges
    graph_builder.add_conditional_edges("chatbot", route_after_chatbot)
    graph_builder.add_edge("tools", "chatbot")  # After tools, go back to LLM

    return graph_builder.compile()


# Pre-compiled graph instance
graph = create_graph()
