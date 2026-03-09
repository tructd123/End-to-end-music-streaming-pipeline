"""
SoundFlow AI Chatbot - ChromaDB Vector Store Setup

Manages the ChromaDB persistent vector store for song/artist embeddings.
"""

import chromadb
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_chroma import Chroma

from config import settings


def get_embedding_function() -> GoogleGenerativeAIEmbeddings:
    """Create the embedding function for vectorizing documents."""
    return GoogleGenerativeAIEmbeddings(
        model=settings.EMBEDDING_MODEL,
        google_api_key=settings.GOOGLE_API_KEY,
    )


def get_vectorstore(collection_name: str | None = None) -> Chroma:
    """Get or create a ChromaDB vector store.

    Args:
        collection_name: Name of the collection. Defaults to songs collection.

    Returns:
        LangChain Chroma vector store instance.
    """
    if collection_name is None:
        collection_name = settings.CHROMA_COLLECTION_SONGS

    return Chroma(
        collection_name=collection_name,
        embedding_function=get_embedding_function(),
        persist_directory=settings.CHROMA_PERSIST_DIR,
    )


def get_chroma_client() -> chromadb.PersistentClient:
    """Get a raw ChromaDB client for direct operations."""
    return chromadb.PersistentClient(path=settings.CHROMA_PERSIST_DIR)


def collection_exists(collection_name: str) -> bool:
    """Check if a ChromaDB collection already has documents."""
    try:
        client = get_chroma_client()
        collection = client.get_collection(collection_name)
        return collection.count() > 0
    except Exception:
        return False
