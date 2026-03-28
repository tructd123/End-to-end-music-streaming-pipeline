"""
SoundFlow AI Chatbot - Search Tools

Search songs and artists from BigQuery data warehouse.
"""

from langchain_core.tools import tool
from google.cloud import bigquery

from config import settings


def _get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=settings.GCP_PROJECT)


@tool
def search_songs(query: str, limit: int = 10) -> str:
    """Tìm kiếm bài hát theo tên trong cơ sở dữ liệu SoundFlow.

    Sử dụng công cụ này khi người dùng muốn:
    - Tìm một bài hát cụ thể theo tên
    - Kiểm tra xem bài hát có trong hệ thống không
    - Xem thông tin chi tiết về một bài hát

    Args:
        query: Tên bài hát hoặc từ khóa tìm kiếm
        limit: Số kết quả tối đa (mặc định 10)
    """
    try:
        client = _get_bq_client()

        sql = f"""
        SELECT
            rank,
            song,
            artist,
            total_plays,
            unique_listeners,
            paid_ratio_pct,
            peak_time_of_day
        FROM `{settings.GCP_PROJECT}.{settings.BQ_DATASET_MARTS}.mart_top_songs`
        WHERE LOWER(song) LIKE CONCAT('%', LOWER(@query), '%')
        ORDER BY total_plays DESC
        LIMIT @limit
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("query", "STRING", query),
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
            ]
        )

        rows = list(client.query(sql, job_config=job_config).result())

        if not rows:
            return f"Không tìm thấy bài hát nào với từ khóa '{query}'."

        results = []
        for row in rows:
            results.append(
                f"#{row.rank} 🎵 **{row.song}** - {row.artist}\n"
                f"   ▶️ {row.total_plays:,} lượt nghe | "
                f"👤 {row.unique_listeners:,} người nghe | "
                f"⏰ Phổ biến: {row.peak_time_of_day}"
            )

        return (
            f"🔍 **Kết quả tìm kiếm cho '{query}'** ({len(rows)} bài):\n\n"
            + "\n\n".join(results)
        )

    except Exception as e:
        return f"Lỗi tìm kiếm: {str(e)}"


@tool
def search_artists(query: str, limit: int = 10) -> str:
    """Tìm kiếm nghệ sĩ theo tên trong cơ sở dữ liệu SoundFlow.

    Sử dụng công cụ này khi người dùng muốn:
    - Tìm nghệ sĩ theo tên
    - Xem thống kê về một nghệ sĩ
    - Biết nghệ sĩ có bao nhiêu bài hát, lượt nghe

    Args:
        query: Tên nghệ sĩ hoặc từ khóa tìm kiếm
        limit: Số kết quả tối đa (mặc định 10)
    """
    try:
        client = _get_bq_client()

        sql = f"""
        SELECT
            rank,
            artist,
            total_songs,
            total_plays,
            total_listeners,
            avg_plays_per_song,
            paid_ratio_pct
        FROM `{settings.GCP_PROJECT}.{settings.BQ_DATASET_MARTS}.mart_top_artists`
        WHERE LOWER(artist) LIKE CONCAT('%', LOWER(@query), '%')
        ORDER BY total_plays DESC
        LIMIT @limit
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("query", "STRING", query),
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
            ]
        )

        rows = list(client.query(sql, job_config=job_config).result())

        if not rows:
            return f"Không tìm thấy nghệ sĩ nào với từ khóa '{query}'."

        results = []
        for row in rows:
            results.append(
                f"#{row.rank} 🎤 **{row.artist}**\n"
                f"   🎵 {row.total_songs} bài | "
                f"▶️ {row.total_plays:,} lượt nghe | "
                f"👤 {row.total_listeners:,} người nghe | "
                f"💎 Paid: {row.paid_ratio_pct}%"
            )

        return (
            f"🔍 **Kết quả tìm kiếm nghệ sĩ '{query}'** ({len(rows)} kết quả):\n\n"
            + "\n\n".join(results)
        )

    except Exception as e:
        return f"Lỗi tìm kiếm: {str(e)}"

@tool
def search_songs_by_artist(artist_name: str, limit: int = 5) -> str:
    """Lấy danh sách các bài hát của một nghệ sĩ cụ thể.

    Sử dụng công cụ này khi người dùng muốn:
    - Nghe nhạc của một nghệ sĩ (ví dụ: "gợi ý bài hát của Justin Bieber", "nhạc của Sơn Tùng")
    - Xem danh sách bài hát của một nghệ sĩ cụ thể

    Args:
        artist_name: Tên nghệ sĩ (ví dụ: "Justin Bieber")
        limit: Số bài hát tối đa trả về (mặc định 5)
    """
    try:
        client = _get_bq_client()

        sql = f"""
        SELECT
            rank,
            song,
            artist,
            total_plays,
            unique_listeners,
            paid_ratio_pct
        FROM `{settings.GCP_PROJECT}.{settings.BQ_DATASET_MARTS}.mart_top_songs`
        WHERE LOWER(artist) LIKE CONCAT('%', LOWER(@artist_name), '%')
        ORDER BY total_plays DESC
        LIMIT @limit
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("artist_name", "STRING", artist_name),        
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
            ]
        )

        rows = list(client.query(sql, job_config=job_config).result())

        if not rows:
            return f"Không tìm thấy bài hát nào của nghệ sĩ '{artist_name}' trong cơ sở dữ liệu."

        results = []
        for i, row in enumerate(rows, 1):
            results.append(
                f"{i}. 🎵 **{row.song}** - {row.artist}\n"
                f"   ▶️ {row.total_plays:,} lượt nghe | "
                f"👤 {row.unique_listeners:,} người nghe | "
                f"💎 Tỉ lệ Paid: {row.paid_ratio_pct}%"
            )

        return (
            f"🎧 **Top bài hát của nghệ sĩ '{artist_name}'**:\n\n"
            + "\n\n".join(results)
        )

    except Exception as e:
        return f"Lỗi tìm kiếm bài hát theo nghệ sĩ: {str(e)}"