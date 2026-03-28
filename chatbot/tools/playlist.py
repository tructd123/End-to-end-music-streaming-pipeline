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


@tool
def delete_playlist(user_id: str, playlist_name: str) -> str:
    """Xóa một playlist của người dùng.

    Sử dụng công cụ này khi người dùng muốn:
    - Xóa một playlist đã tạo
    - Loại bỏ playlist không muốn giữ nữa

    Args:
        user_id: ID của người dùng
        playlist_name: Tên playlist cần xóa
    """
    try:
        client = _get_bq_client()

        table_ref = (
            f"`{settings.GCP_PROJECT}."
            f"{settings.BQ_DATASET_MARTS}.mart_user_playlists`"
        )

        # First check if playlist exists
        check_sql = f"""
        SELECT COUNT(*) as cnt
        FROM {table_ref}
        WHERE user_id = @user_id AND playlist_name = @playlist_name
        """

        check_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "user_id", "STRING", str(user_id)
                ),
                bigquery.ScalarQueryParameter(
                    "playlist_name", "STRING", playlist_name
                ),
            ]
        )

        check_rows = list(
            client.query(check_sql, job_config=check_config).result()
        )
        if not check_rows or check_rows[0].cnt == 0:
            return (
                f"❌ Không tìm thấy playlist **{playlist_name}** "
                f"cho user {user_id}."
            )

        # Delete the playlist
        delete_sql = f"""
        DELETE FROM {table_ref}
        WHERE user_id = @user_id AND playlist_name = @playlist_name
        """

        delete_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "user_id", "STRING", str(user_id)
                ),
                bigquery.ScalarQueryParameter(
                    "playlist_name", "STRING", playlist_name
                ),
            ]
        )

        client.query(delete_sql, job_config=delete_config).result()

        return (
            f"✅ Đã xóa playlist **{playlist_name}** thành công!\n\n"
            f"💡 Bạn có thể tạo playlist mới bằng cách nói: "
            f"\"Tạo playlist [tên] với bài [tên bài hát]\""
        )

    except Exception as e:
        return f"❌ Lỗi khi xóa playlist: {str(e)}"


@tool
def update_playlist(
    user_id: str, playlist_name: str, songs_to_add: str
) -> str:
    """Thêm bài hát vào playlist có sẵn (không trùng lặp).

    Sử dụng công cụ này khi người dùng muốn:
    - Thêm bài hát mới vào playlist đã tạo
    - Cập nhật playlist với bài hát mới

    Args:
        user_id: ID của người dùng
        playlist_name: Tên playlist cần cập nhật
        songs_to_add: Danh sách bài hát mới, phân cách bằng dấu phẩy
    """
    try:
        new_songs = [s.strip() for s in songs_to_add.split(",") if s.strip()]
        if not new_songs:
            return "❌ Vui lòng cung cấp ít nhất một bài hát để thêm."

        client = _get_bq_client()

        table_ref = (
            f"`{settings.GCP_PROJECT}."
            f"{settings.BQ_DATASET_MARTS}.mart_user_playlists`"
        )

        # Get existing playlist
        get_sql = f"""
        SELECT songs
        FROM {table_ref}
        WHERE user_id = @user_id AND playlist_name = @playlist_name
        """

        get_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "user_id", "STRING", str(user_id)
                ),
                bigquery.ScalarQueryParameter(
                    "playlist_name", "STRING", playlist_name
                ),
            ]
        )

        rows = list(client.query(get_sql, job_config=get_config).result())
        if not rows:
            return (
                f"❌ Không tìm thấy playlist **{playlist_name}** "
                f"cho user {user_id}.\n"
                f"💡 Hãy tạo playlist mới bằng cách nói: "
                f"\"Tạo playlist {playlist_name} với bài ...\""
            )

        # Merge songs (no duplicates)
        existing = list(rows[0].songs) if rows[0].songs else []
        existing_lower = {s.lower() for s in existing}
        added = []
        for song in new_songs:
            if song.lower() not in existing_lower:
                existing.append(song)
                existing_lower.add(song.lower())
                added.append(song)

        if not added:
            return (
                f"ℹ️ Tất cả bài hát đã có trong playlist "
                f"**{playlist_name}** rồi."
            )

        if len(existing) > 100:
            return "❌ Playlist không thể có quá 100 bài hát."

        # Update playlist with merged songs
        update_sql = f"""
        UPDATE {table_ref}
        SET songs = @songs, updated_at = CURRENT_TIMESTAMP()
        WHERE user_id = @user_id AND playlist_name = @playlist_name
        """

        update_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "user_id", "STRING", str(user_id)
                ),
                bigquery.ScalarQueryParameter(
                    "playlist_name", "STRING", playlist_name
                ),
                bigquery.ArrayQueryParameter("songs", "STRING", existing),
            ]
        )

        client.query(update_sql, job_config=update_config).result()

        added_display = "\n".join(
            f"   ➕ 🎵 {s}" for s in added
        )

        return (
            f"✅ Đã thêm {len(added)} bài hát vào **{playlist_name}**!\n\n"
            f"{added_display}\n\n"
            f"📋 Playlist hiện có {len(existing)} bài hát."
        )

    except Exception as e:
        return f"❌ Lỗi khi cập nhật playlist: {str(e)}"


@tool
def remove_song_from_playlist(
    user_id: str, playlist_name: str, song_name: str
) -> str:
    """Xóa một bài hát cụ thể khỏi playlist.

    Sử dụng công cụ này khi người dùng muốn:
    - Bỏ một bài hát ra khỏi playlist
    - Loại bỏ bài hát không muốn nghe nữa

    Args:
        user_id: ID của người dùng
        playlist_name: Tên playlist chứa bài hát
        song_name: Tên bài hát cần xóa
    """
    try:
        client = _get_bq_client()

        table_ref = (
            f"`{settings.GCP_PROJECT}."
            f"{settings.BQ_DATASET_MARTS}.mart_user_playlists`"
        )

        # Get existing playlist
        get_sql = f"""
        SELECT songs
        FROM {table_ref}
        WHERE user_id = @user_id AND playlist_name = @playlist_name
        """

        get_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "user_id", "STRING", str(user_id)
                ),
                bigquery.ScalarQueryParameter(
                    "playlist_name", "STRING", playlist_name
                ),
            ]
        )

        rows = list(client.query(get_sql, job_config=get_config).result())
        if not rows:
            return (
                f"❌ Không tìm thấy playlist **{playlist_name}** "
                f"cho user {user_id}."
            )

        existing = list(rows[0].songs) if rows[0].songs else []

        # Find and remove song (case-insensitive match)
        song_lower = song_name.lower()
        new_songs = [s for s in existing if s.lower() != song_lower]

        if len(new_songs) == len(existing):
            return (
                f"❌ Không tìm thấy bài **{song_name}** "
                f"trong playlist **{playlist_name}**."
            )

        # Update playlist without removed song
        update_sql = f"""
        UPDATE {table_ref}
        SET songs = @songs, updated_at = CURRENT_TIMESTAMP()
        WHERE user_id = @user_id AND playlist_name = @playlist_name
        """

        update_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "user_id", "STRING", str(user_id)
                ),
                bigquery.ScalarQueryParameter(
                    "playlist_name", "STRING", playlist_name
                ),
                bigquery.ArrayQueryParameter("songs", "STRING", new_songs),
            ]
        )

        client.query(update_sql, job_config=update_config).result()

        return (
            f"✅ Đã xóa bài **{song_name}** "
            f"khỏi playlist **{playlist_name}**!\n\n"
            f"📋 Playlist còn lại {len(new_songs)} bài hát."
        )

    except Exception as e:
        return f"❌ Lỗi khi xóa bài hát: {str(e)}"

