"""
SoundFlow AI Chatbot - Conversation Memory Store

In-memory session store for maintaining conversation history across requests.
Supports TTL-based cleanup and maximum session limits.
"""

import threading
import time
import uuid
from typing import Optional

from langchain_core.messages import BaseMessage


class ConversationStore:
    """In-memory conversation store with TTL cleanup.

    Attributes:
        ttl_seconds: Time-to-live for inactive sessions (default: 1800 = 30 min).
        max_sessions: Maximum concurrent sessions (default: 1000).
    """

    def __init__(
        self,
        ttl_seconds: int = 1800,
        max_sessions: int = 1000,
    ) -> None:
        self.ttl_seconds = ttl_seconds
        self.max_sessions = max_sessions

        # {conversation_id: {"messages": [...], "last_active": float}}
        self._sessions: dict[str, dict] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_or_create(self, conversation_id: Optional[str] = None) -> tuple[str, list[BaseMessage]]:
        """Get an existing session or create a new one.

        Args:
            conversation_id: Optional session ID. If None or not found, a new
                session is created.

        Returns:
            Tuple of (conversation_id, messages).
        """
        with self._lock:
            self._cleanup_expired()

            if conversation_id and conversation_id in self._sessions:
                # Existing session — update last_active
                session = self._sessions[conversation_id]
                session["last_active"] = time.time()
                return conversation_id, list(session["messages"])

            # Create new session
            if not conversation_id:
                conversation_id = str(uuid.uuid4())

            # Enforce max sessions
            if len(self._sessions) >= self.max_sessions:
                self._evict_oldest()

            self._sessions[conversation_id] = {
                "messages": [],
                "last_active": time.time(),
            }
            return conversation_id, []

    def save(self, conversation_id: str, messages: list[BaseMessage]) -> None:
        """Save updated message history for a session.

        Args:
            conversation_id: The session ID.
            messages: Full message list to store.
        """
        with self._lock:
            self._sessions[conversation_id] = {
                "messages": list(messages),
                "last_active": time.time(),
            }

    def delete(self, conversation_id: str) -> bool:
        """Delete a session. Returns True if it existed."""
        with self._lock:
            return self._sessions.pop(conversation_id, None) is not None

    @property
    def session_count(self) -> int:
        """Number of active sessions."""
        return len(self._sessions)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _cleanup_expired(self) -> int:
        """Remove sessions that exceed TTL. Returns count removed."""
        now = time.time()
        expired = [cid for cid, s in self._sessions.items() if now - s["last_active"] > self.ttl_seconds]
        for cid in expired:
            del self._sessions[cid]
        return len(expired)

    def _evict_oldest(self) -> None:
        """Remove the oldest session (by last_active)."""
        if not self._sessions:
            return
        oldest_id = min(self._sessions, key=lambda cid: self._sessions[cid]["last_active"])
        del self._sessions[oldest_id]
