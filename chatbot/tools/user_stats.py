"""
SoundFlow AI Chatbot - User Statistics Tool

Queries BigQuery mart_active_users to provide user engagement data.
"""

from langchain_core.tools import tool
from google.cloud import bigquery

from config import settings


def _get_bq_client() -> bigquery.Client:
    """Create BigQuery client with project settings."""
    return bigquery.Client(project=settings.GCP_PROJECT)


@tool
def get_user_stats(user_id: str) -> str:
    """Lấy thống kê hoạt động nghe nhạc của một người dùng cụ thể.

    Sử dụng công cụ này khi người dùng muốn:
    - Xem lịch sử nghe nhạc cá nhân
    - Biết engagement tier (Power User, Active, Casual, New)
    - Xem top bài hát/nghệ sĩ yêu thích
    - Xem thống kê tổng quan về hoạt động nghe nhạc

    Args:
        user_id: ID của người dùng cần tra cứu
    """
    try:
        client = _get_bq_client()

        query = f"""
        SELECT
            user_id,
            full_name,
            current_level,
            total_plays,
            unique_songs,
            unique_artists,
            total_sessions,
            active_days,
            avg_plays_per_active_day,
            engagement_tier,
            favorite_time,
            preferred_days,
            morning_plays,
            afternoon_plays,
            evening_plays,
            night_plays,
            first_listen_at,
            last_listen_at,
            listening_span_days,
            is_active
        FROM `{settings.GCP_PROJECT}.{settings.BQ_DATASET_MARTS}.mart_active_users`
        WHERE CAST(user_id AS STRING) = @user_id
        LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_id", "STRING", str(user_id))
            ]
        )

        results = client.query(query, job_config=job_config).result()
        rows = list(results)

        if not rows:
            return f"Không tìm thấy thông tin cho user_id: {user_id}. Vui lòng kiểm tra lại ID."

        user = rows[0]
        return (
            f"📊 **Thống kê của {user.full_name}** (ID: {user.user_id})\n\n"
            f"👤 Gói cước: **{user.current_level.upper()}**\n"
            f"🏆 Engagement: **{user.engagement_tier}**\n"
            f"{'🟢 Đang hoạt động' if user.is_active else '🔴 Không hoạt động'}\n\n"
            f"**📈 Hoạt động nghe nhạc:**\n"
            f"- 🎵 Tổng lượt nghe: **{user.total_plays:,}**\n"
            f"- 🎶 Bài hát khác nhau: **{user.unique_songs:,}**\n"
            f"- 🎤 Nghệ sĩ khác nhau: **{user.unique_artists:,}**\n"
            f"- 📅 Số ngày hoạt động: **{user.active_days}**\n"
            f"- ⚡ Trung bình/ngày: **{user.avg_plays_per_active_day}** lượt\n\n"
            f"**⏰ Thói quen nghe nhạc:**\n"
            f"- Thời điểm yêu thích: **{user.favorite_time}**\n"
            f"- Ngày ưa thích: **{user.preferred_days}**\n"
            f"- 🌅 Sáng: {user.morning_plays} | 🌞 Chiều: {user.afternoon_plays} | "
            f"🌆 Tối: {user.evening_plays} | 🌙 Đêm: {user.night_plays}\n\n"
            f"**📅 Timeline:**\n"
            f"- Nghe đầu tiên: {user.first_listen_at}\n"
            f"- Nghe gần nhất: {user.last_listen_at}\n"
            f"- Tổng thời gian: {user.listening_span_days} ngày"
        )

    except Exception as e:
        return f"Lỗi khi truy vấn dữ liệu người dùng: {str(e)}"


@tool
def get_user_listening_history(user_id: str, limit: int = 10) -> str:
    """Lấy danh sách các bài hát mà người dùng đã nghe gần đây.

    Sử dụng công cụ này khi người dùng hỏi cụ thể về:
    - Những bài hát họ vừa nghe
    - Tên các bài hát hoặc nghệ sĩ trong lịch sử nghe nhạc của họ
    - Lịch sử nghe nhạc gần đây (như bài hát, nghệ sĩ)

    Args:
        user_id: ID của người dùng
        limit: Số lượng bài hát muốn lấy (mặc định 10)
    """
    try:
        staging_dataset = settings.BQ_DATASET_MARTS.replace('marts', 'staging')

        client = _get_bq_client()

        query = f"""
        SELECT
            song,
            artist,
            event_timestamp
        FROM `{settings.GCP_PROJECT}.{staging_dataset}.stg_listens`
        WHERE CAST(user_id AS STRING) = @user_id
          AND song IS NOT NULL
          AND event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        ORDER BY event_timestamp DESC
        LIMIT @limit
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter('user_id', 'STRING', str(user_id)),
                bigquery.ScalarQueryParameter('limit', 'INT64', limit)
            ]
        )

        results = client.query(query, job_config=job_config).result()
        rows = list(results)

        if not rows:
            return f"Không tìm thấy lịch sử nghe nhạc cho user_id: {user_id}. Người dùng có thể chưa nghe bài nào."

        response = f"🕒 **Lịch sử nghe nhạc gần đây của user ID ({user_id}):**\n\n"
        for idx, row in enumerate(rows, 1):
            time_str = row.event_timestamp.strftime('%Y-%m-%d %H:%M:%S') if row.event_timestamp else 'Unknown'
            response += f"{idx}. **{row.song}** - {row.artist} *(lúc {time_str})*\n"

        return response

    except Exception as e:
        return f"Lỗi khi truy vấn lịch sử nghe nhạc: {str(e)}"
