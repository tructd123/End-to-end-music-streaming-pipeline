"""
Tests for ConversationStore - conversation memory module.

Tests:
    - New session creation (no conversation_id)
    - Continue existing session
    - Invalid/unknown conversation_id creates new session
    - TTL expiry removes old sessions
    - Max sessions evicts oldest
"""

import sys
import time

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from memory.store import ConversationStore
from langchain_core.messages import HumanMessage, AIMessage


def test_new_session_without_id():
    """When no conversation_id is given, a new UUID is generated."""
    store = ConversationStore()
    conv_id, messages = store.get_or_create(None)

    assert conv_id is not None
    assert len(conv_id) == 36  # UUID format
    assert messages == []
    assert store.session_count == 1
    print("[OK] New session without ID")


def test_continue_existing_session():
    """When an existing conversation_id is given, messages are returned."""
    store = ConversationStore()
    conv_id, _ = store.get_or_create(None)

    # Save some messages
    msgs = [
        HumanMessage(content="Xin chào"),
        AIMessage(content="Chào bạn! Tôi có thể giúp gì?"),
    ]
    store.save(conv_id, msgs)

    # Retrieve same session
    returned_id, returned_msgs = store.get_or_create(conv_id)
    assert returned_id == conv_id
    assert len(returned_msgs) == 2
    assert returned_msgs[0].content == "Xin chào"
    print("[OK] Continue existing session")


def test_invalid_conversation_id_creates_new():
    """Unknown conversation_id should create a new session with that ID."""
    store = ConversationStore()
    conv_id, messages = store.get_or_create("non-existent-id")

    assert conv_id == "non-existent-id"
    assert messages == []
    assert store.session_count == 1
    print("[OK] Invalid ID creates new session")


def test_ttl_expiry():
    """Sessions older than TTL should be cleaned up."""
    store = ConversationStore(ttl_seconds=1)  # 1 second TTL
    conv_id, _ = store.get_or_create(None)
    store.save(conv_id, [HumanMessage(content="Hello")])

    assert store.session_count == 1

    # Wait for TTL to expire
    time.sleep(1.5)

    # Next get_or_create triggers cleanup
    new_id, msgs = store.get_or_create(None)
    assert new_id != conv_id
    assert msgs == []
    # Old session should be gone, only new one remains
    assert store.session_count == 1
    print("[OK] TTL expiry")


def test_max_sessions_evicts_oldest():
    """When max sessions is reached, the oldest is evicted."""
    store = ConversationStore(max_sessions=3)

    id1, _ = store.get_or_create("session-1")
    store.save("session-1", [HumanMessage(content="msg1")])
    time.sleep(0.01)

    id2, _ = store.get_or_create("session-2")
    store.save("session-2", [HumanMessage(content="msg2")])
    time.sleep(0.01)

    id3, _ = store.get_or_create("session-3")
    store.save("session-3", [HumanMessage(content="msg3")])

    assert store.session_count == 3

    # Adding 4th should evict session-1 (oldest)
    id4, _ = store.get_or_create("session-4")
    assert store.session_count == 3

    # session-1 should be gone
    returned_id, msgs = store.get_or_create("session-1")
    assert msgs == []  # Created fresh — no messages
    print("[OK] Max sessions evicts oldest")


def test_delete_session():
    """Deleting a session removes it from the store."""
    store = ConversationStore()
    conv_id, _ = store.get_or_create(None)
    store.save(conv_id, [HumanMessage(content="Hello")])

    assert store.session_count == 1
    assert store.delete(conv_id) is True
    assert store.session_count == 0
    assert store.delete(conv_id) is False  # Already deleted
    print("[OK] Delete session")


if __name__ == "__main__":
    test_new_session_without_id()
    test_continue_existing_session()
    test_invalid_conversation_id_creates_new()
    test_ttl_expiry()
    test_max_sessions_evicts_oldest()
    test_delete_session()
    print("\n✅ All conversation memory tests passed!")
