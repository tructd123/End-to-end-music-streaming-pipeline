"""
SoundFlow AI Chatbot - Configuration
"""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    """Application settings loaded from environment variables."""

    # LLM Configuration (Google Gemini)
    GOOGLE_API_KEY: str = field(
        default_factory=lambda: os.getenv("GEMINI_API_KEY", "")
    )
    LLM_MODEL: str = field(
        default_factory=lambda: os.getenv("LLM_MODEL", "gemini-2.5-flash")
    )
    EMBEDDING_MODEL: str = field(
        default_factory=lambda: os.getenv(
            "EMBEDDING_MODEL", "models/gemini-embedding-001"
        )
    )

    # GCP Configuration
    GCP_PROJECT: str = field(
        default_factory=lambda: os.getenv("GCP_PROJECT", "")
    )
    GOOGLE_APPLICATION_CREDENTIALS: str = field(
        default_factory=lambda: os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    )

    # BigQuery Configuration
    BQ_DATASET_MARTS: str = field(
        default_factory=lambda: os.getenv("BQ_DATASET_MARTS", "marts")
    )

    # ChromaDB Configuration
    CHROMA_PERSIST_DIR: str = field(
        default_factory=lambda: os.getenv(
            "CHROMA_PERSIST_DIR", "./chroma_data"
        )
    )
    CHROMA_COLLECTION_SONGS: str = "soundflow_songs"
    CHROMA_COLLECTION_ARTISTS: str = "soundflow_artists"

    # RAG Configuration
    RAG_TOP_K: int = 10
    RAG_SCORE_THRESHOLD: float = 0.7

    # API Configuration
    API_HOST: str = field(
        default_factory=lambda: os.getenv("API_HOST", "0.0.0.0")
    )
    API_PORT: int = field(
        default_factory=lambda: int(os.getenv("API_PORT", "8000"))
    )

    def validate(self) -> list[str]:
        """Validate required settings and return list of missing ones."""
        missing = []
        if not self.GOOGLE_API_KEY:
            missing.append("GEMINI_API_KEY")
        if not self.GCP_PROJECT:
            missing.append("GCP_PROJECT")
        return missing


settings = Settings()
