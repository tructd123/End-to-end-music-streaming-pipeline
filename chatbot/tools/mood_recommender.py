"""
SoundFlow AI Chatbot - Mood-based Recommendation Tool

Analyzes mood/sentiment from user messages using Gemini,
maps to music keywords, and searches ChromaDB for matching songs.
"""

from langchain_core.tools import tool
from langchain_google_genai import ChatGoogleGenerativeAI

from rag.retriever import get_retriever
from rag.vectorstore import collection_exists
from config import settings


# ---------------------------------------------------------------------------
# Mood categories and keyword mapping (Task 2.2)
# ---------------------------------------------------------------------------
MOOD_KEYWORDS: dict[str, list[str]] = {
    "happy": ["dance", "upbeat", "pop", "party", "fun", "joyful", "vui"],
    "sad": ["ballad", "slow", "acoustic", "emotional", "chill", "buồn"],
    "energetic": ["dance", "upbeat", "EDM", "pop", "party", "rock", "sôi động"],
    "relaxed": ["chill", "acoustic", "lofi", "jazz", "soft", "thư giãn"],
    "romantic": ["love", "ballad", "R&B", "romantic", "tình yêu", "lãng mạn"],
    "angry": ["rock", "metal", "rap", "intense", "powerful", "mạnh mẽ"],
    "nostalgic": ["classic", "retro", "old", "vintage", "hoài niệm", "xưa"],
    "focus": ["lofi", "instrumental", "ambient", "study", "piano", "tập trung"],
}

MOOD_DISPLAY: dict[str, str] = {
    "happy": "😊 Vui vẻ",
    "sad": "😢 Buồn",
    "energetic": "⚡ Năng động",
    "relaxed": "😌 Thư giãn",
    "romantic": "💕 Lãng mạn",
    "angry": "😤 Giận dữ",
    "nostalgic": "🕰️ Hoài niệm",
    "focus": "🎯 Tập trung",
}


def _fmt_int(value) -> str:
    """Format int-like values safely for display."""
    try:
        if value is None:
            return "N/A"
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "N/A"


def _classify_mood(message: str) -> str | None:
    """Use Gemini to classify mood from user message.

    Returns one of the MOOD_KEYWORDS keys, or None if ambiguous.
    """
    llm = ChatGoogleGenerativeAI(
        model=settings.LLM_MODEL,
        google_api_key=settings.GOOGLE_API_KEY,
        temperature=0.0,
    )

    valid_moods = ", ".join(MOOD_KEYWORDS.keys())
    prompt = (
        f"Classify the mood/emotion of the following message into "
        f"exactly ONE of these categories: {valid_moods}.\n\n"
        f"Message: \"{message}\"\n\n"
        f"Rules:\n"
        f"- Reply with ONLY the category name (one word, lowercase)\n"
        f"- If the mood is unclear or the message is not about emotions, "
        f"reply with 'unclear'\n"
        f"- Examples: 'Tôi buồn' -> sad, "
        f"'Cho tôi nhạc sôi động' -> energetic, "
        f"'Tôi muốn thư giãn' -> relaxed"
    )

    response = llm.invoke(prompt)
    mood = response.content.strip().lower()

    if mood in MOOD_KEYWORDS:
        return mood
    return None


@tool
def recommend_by_mood(message: str, top_k: int = 5) -> str:
    """Gợi ý bài hát theo tâm trạng/mood của người dùng.

    Sử dụng công cụ này khi người dùng:
    - Diễn tả cảm xúc hoặc tâm trạng (vui, buồn, giận, mệt...)
    - Muốn nhạc phù hợp mood hiện tại
    - Nói "Tôi buồn", "Cho nhạc vui vẻ", "Tôi cần thư giãn"...

    Args:
        message: Tin nhắn chứa biểu cảm/tâm trạng của người dùng
        top_k: Số lượng bài hát gợi ý (mặc định 5)
    """
    try:
        top_k = max(1, min(int(top_k), 20))

        # Step 1: Classify mood using Gemini
        mood = _classify_mood(message)

        if mood is None:
            return (
                "🤔 Mình chưa rõ tâm trạng của bạn. "
                "Bạn có thể mô tả cụ thể hơn không?\n\n"
                "Ví dụ:\n"
                "- 😢 \"Tôi đang buồn\"\n"
                "- ⚡ \"Tôi muốn nhạc sôi động\"\n"
                "- 😌 \"Tôi cần thư giãn\"\n"
                "- 💕 \"Tôi đang yêu\"\n"
                "- 🎯 \"Tôi cần tập trung học bài\""
            )

        # Step 2: Get keywords for the detected mood
        keywords = MOOD_KEYWORDS[mood]
        search_query = " ".join(keywords)

        # Step 3: Check RAG readiness
        rag_ready = bool(settings.GOOGLE_API_KEY) and collection_exists(
            settings.CHROMA_COLLECTION_SONGS
        )

        if not rag_ready:
            mood_label = MOOD_DISPLAY.get(mood, mood)
            return (
                f"🎭 Tâm trạng phát hiện: **{mood_label}**\n\n"
                f"⚠️ RAG chưa sẵn sàng để gợi ý nhạc theo mood. "
                f"Hãy chạy `python -m rag.ingest --force`."
            )

        # Step 4: Search ChromaDB with mood keywords
        retriever = get_retriever(top_k=top_k)
        docs = retriever.invoke(search_query)

        if not docs:
            mood_label = MOOD_DISPLAY.get(mood, mood)
            return (
                f"🎭 Tâm trạng: **{mood_label}**\n\n"
                f"Không tìm thấy bài hát phù hợp tâm trạng này. "
                f"Hãy thử mô tả khác."
            )

        # Step 5: Format results
        mood_label = MOOD_DISPLAY.get(mood, mood)
        results = []
        for i, doc in enumerate(docs, 1):
            metadata = doc.metadata
            results.append(
                f"{i}. 🎵 **{metadata.get('song', 'N/A')}** - "
                f"{metadata.get('artist', 'N/A')}\n"
                f"   ▶️ Lượt nghe: {_fmt_int(metadata.get('total_plays'))} | "
                f"👤 Người nghe: "
                f"{_fmt_int(metadata.get('unique_listeners'))} | "
                f"💎 Tỉ lệ Paid: {metadata.get('paid_ratio_pct', 'N/A')}%"
            )

        return (
            f"🎭 Tâm trạng: **{mood_label}**\n"
            f"🔍 Từ khóa: {', '.join(keywords[:4])}\n\n"
            + "\n\n".join(results)
        )

    except Exception as e:
        return f"Lỗi khi gợi ý nhạc theo mood: {str(e)}. Hãy thử lại sau."
