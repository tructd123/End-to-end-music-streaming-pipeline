"""
SoundFlow AI Chatbot - LangGraph Workflow

Implements the ReAct agent pattern:
  User message → LLM (chatbot_node) → Tool calls? → tools_node → LLM → Response
"""

from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode

from agent.state import AgentState
from agent.prompts import SOUNDFLOW_SYSTEM_PROMPT
from tools.song_recommender import recommend_songs
from tools.user_stats import get_user_stats
from tools.subscription import change_subscription
from tools.search import search_songs, search_artists
from tools.playlist import get_playlist, create_playlist
from config import settings


# ---------------------------------------------------------------------------
# All tools available to the agent
# ---------------------------------------------------------------------------
ALL_TOOLS = [
    recommend_songs,
    get_user_stats,
    change_subscription,
    search_songs,
    search_artists,
    get_playlist,
    create_playlist,
]


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------
def chatbot_node(state: AgentState) -> dict:
    """Main LLM node that processes messages and decides whether to call tools."""

    llm = ChatGoogleGenerativeAI(
        model=settings.LLM_MODEL,
        google_api_key=settings.GOOGLE_API_KEY,
        temperature=0.7,
    )

    # Bind tools so the LLM can generate tool calls
    llm_with_tools = llm.bind_tools(ALL_TOOLS)

    # Prepend system prompt
    from langchain_core.messages import SystemMessage

    messages = state["messages"]
    if not messages or not isinstance(messages[0], SystemMessage):
        messages = [SystemMessage(content=SOUNDFLOW_SYSTEM_PROMPT)] + messages

    response = llm_with_tools.invoke(messages)
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
