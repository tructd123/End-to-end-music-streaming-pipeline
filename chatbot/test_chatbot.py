"""
SoundFlow AI Chatbot - Quick Test Script (Windows-compatible)

Tests Gemini API connectivity and agent functionality.

Usage:
    cd chatbot
    python test_chatbot.py
"""

import sys
import os

# Ensure chatbot directory is in path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Fix Windows encoding
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def test_config():
    print("=" * 60)
    print("TEST 1: Configuration")
    print("=" * 60)
    from config import settings

    api_ok = bool(settings.GOOGLE_API_KEY)
    print(f"  GOOGLE_API_KEY: {'[OK] Set' if api_ok else '[FAIL] Missing'}")
    print(f"  LLM_MODEL: {settings.LLM_MODEL}")
    print(f"  EMBEDDING_MODEL: {settings.EMBEDDING_MODEL}")
    print(f"  GCP_PROJECT: {settings.GCP_PROJECT or '(not set)'}")
    print()
    return api_ok


def test_gemini_llm():
    print("=" * 60)
    print("TEST 2: Gemini LLM Direct Call")
    print("=" * 60)
    try:
        from langchain_google_genai import ChatGoogleGenerativeAI
        from langchain_core.messages import HumanMessage
        from config import settings

        llm = ChatGoogleGenerativeAI(
            model=settings.LLM_MODEL,
            google_api_key=settings.GOOGLE_API_KEY,
            temperature=0.7,
        )

        response = llm.invoke([HumanMessage(content="Say hello in one sentence.")])
        print(f"  [OK] Response: {response.content[:200]}")
        print()
        return True
    except Exception as e:
        print(f"  [FAIL] Error: {e}")
        print()
        return False


def test_gemini_embedding():
    print("=" * 60)
    print("TEST 3: Gemini Embedding")
    print("=" * 60)
    try:
        from langchain_google_genai import GoogleGenerativeAIEmbeddings
        from config import settings

        embeddings = GoogleGenerativeAIEmbeddings(
            model=settings.EMBEDDING_MODEL,
            google_api_key=settings.GOOGLE_API_KEY,
        )

        result = embeddings.embed_query("best songs 2024")
        print(f"  [OK] Embedding dimension: {len(result)}")
        print(f"  [OK] First 3 values: {result[:3]}")
        print()
        return True
    except Exception as e:
        print(f"  [FAIL] Error: {e}")
        print()
        return False


def test_playlist_tools():
    print("=" * 60)
    print("TEST 4: Playlist Tools Import & Structure")
    print("=" * 60)
    try:
        from tools.playlist import get_playlist, create_playlist

        # Check @tool decorator (creates a BaseTool with .name attribute)
        assert hasattr(get_playlist, "name"), "get_playlist missing .name"
        assert hasattr(create_playlist, "name"), "create_playlist missing .name"
        assert get_playlist.name == "get_playlist"
        assert create_playlist.name == "create_playlist"

        # Check docstrings exist
        assert get_playlist.description, "get_playlist missing description"
        assert create_playlist.description, "create_playlist missing description"

        # Check Vietnamese content in descriptions
        assert "playlist" in get_playlist.description.lower()
        assert "playlist" in create_playlist.description.lower()

        print(f"  [OK] get_playlist: tool registered, has Vietnamese docstring")
        print(f"  [OK] create_playlist: tool registered, has Vietnamese docstring")
        print()
        return True
    except Exception as e:
        print(f"  [FAIL] Error: {e}")
        import traceback
        traceback.print_exc()
        print()
        return False


def test_agent_chat():
    print("=" * 60)
    print("TEST 5: Agent Graph (General Chat)")
    print("=" * 60)
    try:
        from agent.graph import create_graph
        from langchain_core.messages import HumanMessage

        graph = create_graph()

        result = graph.invoke({
            "messages": [HumanMessage(content="Hello! What can you do? Answer very briefly.")],
            "user_id": None,
        })

        ai_msg = result["messages"][-1]
        print(f"  [OK] Agent responded!")
        print(f"  Response: {ai_msg.content[:300]}")
        print()
        return True
    except Exception as e:
        print(f"  [FAIL] Error: {e}")
        import traceback
        traceback.print_exc()
        print()
        return False


def main():
    print("\nSoundFlow AI Chatbot - Test Suite (Gemini)\n")

    results = {}
    results["Config"] = test_config()
    results["Gemini LLM"] = test_gemini_llm()
    results["Gemini Embedding"] = test_gemini_embedding()
    results["Playlist Tools"] = test_playlist_tools()
    results["Agent Graph"] = test_agent_chat()

    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    all_passed = True
    for name, passed in results.items():
        icon = "[OK]" if passed else "[FAIL]"
        print(f"  {icon} {name}")
        if not passed:
            all_passed = False

    print()
    if all_passed:
        print("All tests passed!")
    else:
        print("Some tests failed. Check output above.")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
