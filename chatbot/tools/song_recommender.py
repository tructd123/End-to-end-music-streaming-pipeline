"""
SoundFlow AI Chatbot - Song Recommendation Tool

Uses RAG (Retrieval-Augmented Generation) to recommend songs
based on user preferences and the SoundFlow music catalog.
"""

from google.cloud import bigquery
from langchain_core.tools import tool

from config import settings
from rag.retriever import get_retriever
from rag.vectorstore import collection_exists


def _fmt_int(value) -> str:
    """Format int-like values safely for user display."""
    try:
        if value is None:
            return "N/A"
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "N/A"


def _looks_like_trending_query(query: str) -> bool:
    q = (query or "").lower()
    keywords = [
        # --- Danh sách cũ của bạn ---
        "thịnh hành",
        "thinh hanh",
        "trending",
        "hot",
        "phổ biến",
        "pho bien",
        "top",
        "nghe nhiều nhất",
        "nghe nhieu nhat",
        "hay nhất",
        "nổi tiếng",
        # --- Nhóm: Chất lượng & Cảm xúc (Quality & Feelings) ---
        "hay nhat",  # Bổ sung bản không dấu cho "hay nhất"
        "đỉnh nhất",
        "dinh nhat",
        "tuyệt nhất",
        "tuyet nhat",
        "được yêu thích",
        "duoc yeu thich",
        "yêu thích nhất",
        "yeu thich nhat",
        "bất hủ",
        "bat hu",  # Dành cho nhạc xưa/kinh điển
        "đỉnh của chóp",  # Slang gen Z hay dùng
        # --- Nhóm: Xu hướng & Bảng xếp hạng (Trends & Charts) ---
        "xu hướng",
        "xu huong",
        "bảng xếp hạng",
        "bang xep hang",
        "bxh",
        "hit",
        "siêu hit",
        "sieu hit",
        "đình đám",
        "dinh dam",
        "đang nổi",
        "dang noi",
        "viral",
        # --- Nhóm: Lượt xem/Nghe (Views & Streams) ---
        "triệu view",
        "trieu view",
        "nhiều lượt nghe",
        "nhieu luot nghe",
        "nhiều view",
        "nhieu view",
        "best",
        "top 10",
        "top 100",
    ]
    return any(k in q for k in keywords)


def _fetch_trending_from_bigquery(limit: int) -> str:
    """Fallback path: query top songs directly from BigQuery marts."""
    client = bigquery.Client(project=settings.GCP_PROJECT)

    sql = f"""
    SELECT
        rank,
        song,
        artist,
        total_plays,
        unique_listeners,
        paid_ratio_pct
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
    if not rows:
        return "Không có dữ liệu bài hát thịnh hành trong BigQuery."

    lines = []
    for i, row in enumerate(rows, 1):
        lines.append(
            f"{i}. 🎵 **{row.song}** - {row.artist}\n"
            f"   ▶️ Lượt nghe: {_fmt_int(row.total_plays)} | "
            f"👤 Người nghe: {_fmt_int(row.unique_listeners)} | "
            f"💎 Tỉ lệ Paid: {row.paid_ratio_pct}%"
        )

    return "🔥 **Top bài hát thịnh hành** (fallback từ BigQuery):\n\n" + "\n\n".join(lines)


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
        top_k = max(1, min(int(top_k), 20))

        # Always route trending/top requests to BigQuery directly for accurate metrics
        if _looks_like_trending_query(query):
            return _fetch_trending_from_bigquery(top_k)

        # If RAG dependencies are not ready, fallback for trending requests.
        rag_ready = bool(settings.GOOGLE_API_KEY) and collection_exists(settings.CHROMA_COLLECTION_SONGS)

        if not rag_ready:
            return (
                "RAG chưa sẵn sàng để gợi ý theo ngữ nghĩa. "
                "Hãy kiểm tra GEMINI_API_KEY và chạy `python -m rag.ingest --force` "
                "trong thư mục chatbot."
            )

        retriever = get_retriever(top_k=top_k)
        docs = retriever.invoke(query)

        if not docs:
            return "Không tìm thấy bài hát phù hợp với yêu cầu. Hãy thử mô tả khác."

        results = []
        for i, doc in enumerate(docs, 1):
            metadata = doc.metadata
            results.append(
                f"{i}. 🎵 **{metadata.get('song', 'N/A')}** - {metadata.get('artist', 'N/A')}\n"
                f"   ▶️ Lượt nghe: {_fmt_int(metadata.get('total_plays'))} | "
                f"👤 Người nghe: {_fmt_int(metadata.get('unique_listeners'))} | "
                f"💎 Tỉ lệ Paid: {metadata.get('paid_ratio_pct', 'N/A')}%"
            )

        return f"🎶 **Top {len(docs)} bài hát phù hợp:**\n\n" + "\n\n".join(results)

    except Exception as e:
        return f"Lỗi khi tìm kiếm bài hát: {str(e)}. Hãy thử lại sau."
