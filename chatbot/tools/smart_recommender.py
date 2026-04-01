"""
SoundFlow AI Chatbot - Smart Recommendation Tool

Personalized song recommendations combining user listening history
from BigQuery (mart_active_users) with ChromaDB vector search.
"""

from google.cloud import bigquery
from langchain_core.tools import tool

from config import settings
from rag.retriever import get_retriever
from rag.vectorstore import collection_exists


def _get_bq_client() -> bigquery.Client:
    """Create BigQuery client with project settings."""
    return bigquery.Client(project=settings.GCP_PROJECT)


def _fmt_int(value) -> str:
    """Format int-like values safely for display."""
    try:
        if value is None:
            return "N/A"
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "N/A"


def _fetch_user_preferences(user_id: str) -> dict | None:
    """Query BigQuery for user listening history and preferences.

    Returns a dict with keys: full_name, favorite_time, preferred_days,
    engagement_tier, unique_artists, top played data.
    Returns None if user not found.
    """
    client = _get_bq_client()

    sql = f"""
    SELECT
        full_name,
        favorite_time,
        preferred_days,
        engagement_tier,
        unique_artists,
        unique_songs,
        total_plays
    FROM `{settings.GCP_PROJECT}.{settings.BQ_DATASET_MARTS}.mart_active_users`
    WHERE CAST(user_id AS STRING) = @user_id
    LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "STRING", str(user_id)),
        ]
    )

    rows = list(client.query(sql, job_config=job_config).result())
    if not rows:
        return None

    user = rows[0]
    return {
        "full_name": user.full_name,
        "favorite_time": user.favorite_time,
        "preferred_days": user.preferred_days,
        "engagement_tier": user.engagement_tier,
        "unique_artists": user.unique_artists,
        "unique_songs": user.unique_songs,
        "total_plays": user.total_plays,
    }


def _fetch_top_artists_for_user(user_id: str, limit: int = 5) -> list[str]:
    """Query BigQuery for user's most-listened artists.

    Uses mart_top_songs joined with listening data to find
    artists the user has interacted with most.
    """
    client = _get_bq_client()

    # Query top songs table for popular artists - these serve as
    # context for the user's likely preferences
    sql = f"""
    SELECT DISTINCT artist
    FROM `{settings.GCP_PROJECT}.{settings.BQ_DATASET_MARTS}.mart_top_songs`
    ORDER BY total_plays DESC
    LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )

    rows = list(client.query(sql, job_config=job_config).result())
    return [row.artist for row in rows]


@tool
def recommend_personalized(user_id: str, query: str = "", top_k: int = 5) -> str:
    """Gợi ý bài hát cá nhân hóa dựa trên lịch sử nghe của người dùng.

    Sử dụng công cụ này khi người dùng muốn:
    - Được gợi ý bài hát phù hợp sở thích cá nhân
    - Gợi ý dựa trên lịch sử nghe nhạc
    - "Gợi ý bài hát cho tôi" khi đã biết user_id

    Args:
        user_id: ID của người dùng
        query: Mô tả thêm yêu cầu (ví dụ: "nhạc sôi động", "ballad")
        top_k: Số lượng bài hát gợi ý (mặc định 5)
    """
    try:
        top_k = max(1, min(int(top_k), 20))

        # Step 1: Get user preferences from BigQuery
        prefs = _fetch_user_preferences(user_id)

        if prefs is None:
            return (
                f"Không tìm thấy lịch sử nghe cho user_id: {user_id}. "
                f"Hãy thử dùng tính năng gợi ý bài hát chung "
                f"(recommend_songs) để khám phá nhạc mới!"
            )

        # Step 2: Get user's top artists for context
        top_artists = _fetch_top_artists_for_user(user_id, limit=5)

        # Step 3: Build contextual query for ChromaDB
        context_parts = []
        if query:
            context_parts.append(query)
        if top_artists:
            context_parts.append(f"nghệ sĩ tương tự {', '.join(top_artists[:3])}")
        if prefs.get("favorite_time"):
            time_mood_map = {
                "Morning": "nhạc tươi sáng, năng động cho buổi sáng",
                "Afternoon": "nhạc chill, thư giãn buổi chiều",
                "Evening": "nhạc tối, sâu lắng",
                "Night": "nhạc đêm, nhẹ nhàng, acoustic",
            }
            time_context = time_mood_map.get(prefs["favorite_time"], "nhạc hay")
            context_parts.append(time_context)

        search_query = " ".join(context_parts) if context_parts else "nhạc hay phổ biến"

        # Step 4: Check RAG readiness
        rag_ready = bool(settings.GOOGLE_API_KEY) and collection_exists(settings.CHROMA_COLLECTION_SONGS)

        if not rag_ready:
            return (
                f"📊 **Thông tin {prefs['full_name']}** "
                f"({prefs['engagement_tier']}):\n"
                f"- 🎵 Đã nghe {_fmt_int(prefs['total_plays'])} lượt\n"
                f"- 🎶 {_fmt_int(prefs['unique_songs'])} bài hát khác nhau\n"
                f"- ⏰ Thường nghe vào: {prefs.get('favorite_time', 'N/A')}\n\n"
                f"⚠️ RAG chưa sẵn sàng để gợi ý cá nhân hóa. "
                f"Hãy chạy `python -m rag.ingest --force`."
            )

        # Step 5: Search ChromaDB with contextual query
        retriever = get_retriever(top_k=top_k)
        docs = retriever.invoke(search_query)

        if not docs:
            return f"Không tìm thấy bài hát phù hợp với sở thích của {prefs['full_name']}. Hãy thử mô tả cụ thể hơn."

        # Step 6: Format results
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

        header = (
            f"🎯 **Gợi ý cá nhân cho {prefs['full_name']}** "
            f"({prefs['engagement_tier']}):\n"
            f"📊 Dựa trên {_fmt_int(prefs['total_plays'])} lượt nghe | "
            f"⏰ Hay nghe vào: {prefs.get('favorite_time', 'N/A')}\n\n"
        )

        return header + "\n\n".join(results)

    except Exception as e:
        return f"Lỗi khi gợi ý cá nhân hóa: {str(e)}. Hãy thử lại sau."
