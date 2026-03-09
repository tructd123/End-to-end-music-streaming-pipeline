"""
SoundFlow AI Chatbot - Song Recommendation Tool

Uses RAG (Retrieval-Augmented Generation) to recommend songs
based on user preferences and the SoundFlow music catalog.
"""

from langchain_core.tools import tool

from rag.retriever import get_retriever


@tool
def recommend_songs(query: str, top_k: int = 5) -> str:
    """Tư vấn và gợi ý bài hát dựa trên yêu cầu của người dùng.

    Sử dụng công cụ này khi người dùng muốn:
    - Được gợi ý bài hát phổ biến, trending
    - Tìm bài hát theo mood, thể loại, hoặc nghệ sĩ
    - Khám phá nhạc mới

    Args:
        query: Mô tả yêu cầu bài hát (ví dụ: "bài hát sôi động", "nhạc của Adele")
        top_k: Số lượng bài hát cần gợi ý (mặc định 5)
    """
    try:
        retriever = get_retriever(top_k=top_k)
        docs = retriever.invoke(query)

        if not docs:
            return "Không tìm thấy bài hát phù hợp với yêu cầu. Hãy thử mô tả khác."

        results = []
        for i, doc in enumerate(docs, 1):
            metadata = doc.metadata
            results.append(
                f"{i}. 🎵 **{metadata.get('song', 'N/A')}** - {metadata.get('artist', 'N/A')}\n"
                f"   ▶️ Lượt nghe: {metadata.get('total_plays', 'N/A'):,} | "
                f"👤 Người nghe: {metadata.get('unique_listeners', 'N/A'):,} | "
                f"💎 Tỉ lệ Paid: {metadata.get('paid_ratio_pct', 'N/A')}%"
            )

        return (
            f"🎶 **Top {len(docs)} bài hát phù hợp:**\n\n"
            + "\n\n".join(results)
        )

    except Exception as e:
        return f"Lỗi khi tìm kiếm bài hát: {str(e)}. Hãy thử lại sau."
