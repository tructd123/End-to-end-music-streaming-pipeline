"""
SoundFlow AI Chatbot - Playlist Management Tools

Create and view user playlists stored in BigQuery.
"""

from langchain_core.tools import tool
from google.cloud import bigquery

from config import settings


def _get_bq_client() -> bigquery.Client:
    """Create BigQuery client with project settings."""
    return bigquery.Client(project=settings.GCP_PROJECT)


@tool
def get_playlist(user_id: str) -> str:
    """Xem danh sách playlist của một người dùng.

    Sử dụng công cụ này khi người dùng muốn:
    - Xem các playlist đã tạo
    - Kiểm tra nội dung playlist
    - Liệt kê tất cả playlist của mình

    Args:
        user_id: ID của người dùng cần xem playlist
    """
    try:
        client = _get_bq_client()

        sql = f"""
        SELECT
            playlist_name,
            songs,
            created_at,
            updated_at
        FROM `{settings.GCP_PROJECT}.{settings.BQ_DATASET_MARTS}.mart_user_playlists`
        WHERE user_id = @user_id
        ORDER BY updated_at DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_id", "STRING", str(user_id)),
            ]
        )

        rows = list(client.query(sql, job_config=job_config).result())

        if not rows:
            return (
                f"📋 Người dùng (ID: {user_id}) chưa có playlist nào.\n"
                f"💡 Bạn có thể tạo playlist mới bằng cách nói: "
                f"\"Tạo playlist [tên] với bài [tên bài hát]\""
            )

        results = []
        for i, row in enumerate(rows, 1):
            songs_list = list(row.songs) if row.songs else []
            songs_display = ", ".join(songs_list[:5])
            if len(songs_list) > 5:
                songs_display += f" ... (+{len(songs_list) - 5} bài nữa)"

            results.append(
                f"**{i}. 🎵 {row.playlist_name}**\n"
                f"   📝 {len(songs_list)} bài hát: {songs_display}\n"
                f"   📅 Tạo: {row.created_at} | Cập nhật: {row.updated_at}"
            )

        return (
            f"📋 **Playlist của bạn** (ID: {user_id}) — {len(rows)} playlist:\n\n"
            + "\n\n".join(results)
        )

    except Exception as e:
        return f"❌ Lỗi khi lấy playlist: {str(e)}"


@tool
def create_playlist(user_id: str, playlist_name: str, songs: str) -> str:
    """Tạo playlist mới hoặc cập nhật playlist đã có cho người dùng.

    Sử dụng công cụ này khi người dùng muốn:
    - Tạo một playlist mới với các bài hát
    - Thêm bài hát vào playlist hiện tại
    - Lưu danh sách bài hát yêu thích

    Args:
        user_id: ID của người dùng tạo playlist
        playlist_name: Tên playlist (ví dụ: "Nhạc buồn", "Workout Mix")
        songs: Danh sách bài hát, phân cách bằng dấu phẩy (ví dụ: "Hạ Trắng, Diễm Xưa, Nối Vòng Tay Lớn")
    """
    try:
        # Parse and clean song list
        songs_list = [s.strip() for s in songs.split(",") if s.strip()]

        if not songs_list:
            return "❌ Vui lòng cung cấp ít nhất một bài hát cho playlist."

        if len(songs_list) > 100:
            return "❌ Playlist không thể có quá 100 bài hát. Vui lòng giảm số lượng."

        client = _get_bq_client()

        table_ref = f"`{settings.GCP_PROJECT}.{settings.BQ_DATASET_MARTS}.mart_user_playlists`"

        # MERGE: update if playlist exists, insert if new
        sql = f"""
        MERGE {table_ref} AS target
        USING (
            SELECT
                @user_id AS user_id,
                @playlist_name AS playlist_name
        ) AS source
        ON target.user_id = source.user_id
           AND target.playlist_name = source.playlist_name
        WHEN MATCHED THEN
            UPDATE SET
                songs = @songs,
                updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (user_id, playlist_name, songs, created_at, updated_at)
            VALUES (
                @user_id,
                @playlist_name,
                @songs,
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            )
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_id", "STRING", str(user_id)),
                bigquery.ScalarQueryParameter("playlist_name", "STRING", playlist_name),
                bigquery.ArrayQueryParameter("songs", "STRING", songs_list),
            ]
        )

        client.query(sql, job_config=job_config).result()

        songs_display = "\n".join(f"   {i}. 🎵 {s}" for i, s in enumerate(songs_list, 1))

        return (
            f"✅ **Playlist đã được lưu thành công!**\n\n"
            f"📋 **{playlist_name}**\n"
            f"👤 User: {user_id}\n"
            f"🎶 Số bài hát: {len(songs_list)}\n\n"
            f"{songs_display}\n\n"
            f"💡 Xem playlist bằng cách hỏi: \"Xem playlist của tôi\""
        )

    except Exception as e:
        return f"❌ Lỗi khi tạo playlist: {str(e)}"
