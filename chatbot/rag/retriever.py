"""
SoundFlow AI Chatbot - RAG Retriever

Configures the retriever that searches the ChromaDB vector store
for relevant songs/artists based on user queries.
"""

from langchain_core.vectorstores import VectorStoreRetriever

from config import settings
from rag.vectorstore import get_vectorstore


def get_retriever(
    collection_name: str | None = None,
    top_k: int | None = None,
) -> VectorStoreRetriever:
    """Create a retriever from the ChromaDB vector store.

    Args:
        collection_name: Collection to search. Defaults to songs collection.
        top_k: Number of results to return. Defaults to settings.RAG_TOP_K.

    Returns:
        LangChain retriever configured for similarity search.
    """
    if top_k is None:
        top_k = settings.RAG_TOP_K

    vectorstore = get_vectorstore(collection_name=collection_name)

    return vectorstore.as_retriever(
        search_type="similarity",
        search_kwargs={"k": top_k},
    )
