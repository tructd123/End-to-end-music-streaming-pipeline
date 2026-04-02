"""
Tests for Phase 2 Smart Features:
- smart_recommender.recommend_personalized
- mood_recommender.recommend_by_mood
- playlist CRUD (delete_playlist, update_playlist, remove_song_from_playlist)
"""

from unittest.mock import MagicMock, patch


# -----------------------------------------------------------------------
# Smart Recommender Tests
# -----------------------------------------------------------------------
class TestRecommendPersonalized:
    """Tests for recommend_personalized tool."""

    @patch("tools.smart_recommender._fetch_user_preferences")
    def test_user_not_found(self, mock_prefs):
        """Verify fallback when user has no listening history."""
        from tools.smart_recommender import recommend_personalized

        mock_prefs.return_value = None
        result = recommend_personalized.invoke({"user_id": "unknown123", "query": "nhạc hay"})
        assert "recommend_songs" in result.lower() or "chung" in result.lower() or "Không tìm thấy" in result

    @patch("tools.smart_recommender.collection_exists")
    @patch("tools.smart_recommender._fetch_top_artists_for_user")
    @patch("tools.smart_recommender._fetch_user_preferences")
    def test_rag_not_ready(self, mock_prefs, mock_artists, mock_coll):
        """Verify message when RAG is not available."""
        from tools.smart_recommender import recommend_personalized

        mock_prefs.return_value = {
            "full_name": "Test User",
            "favorite_time": "Evening",
            "preferred_days": "Weekend",
            "engagement_tier": "Active",
            "unique_artists": 10,
            "unique_songs": 50,
            "total_plays": 200,
        }
        mock_artists.return_value = ["Artist1"]
        mock_coll.return_value = False

        result = recommend_personalized.invoke({"user_id": "user1", "query": "nhạc"})
        assert "RAG" in result or "chưa sẵn sàng" in result

    @patch("tools.smart_recommender.get_retriever")
    @patch("tools.smart_recommender.collection_exists")
    @patch("tools.smart_recommender._fetch_top_artists_for_user")
    @patch("tools.smart_recommender._fetch_user_preferences")
    def test_successful_recommendation(self, mock_prefs, mock_artists, mock_coll, mock_retriever):
        """Verify tool returns personalized results."""
        from tools.smart_recommender import recommend_personalized

        mock_prefs.return_value = {
            "full_name": "Nguyễn Văn A",
            "favorite_time": "Night",
            "preferred_days": "Weekend",
            "engagement_tier": "Power User",
            "unique_artists": 30,
            "unique_songs": 100,
            "total_plays": 500,
        }
        mock_artists.return_value = ["Sơn Tùng MTP"]
        mock_coll.return_value = True

        mock_doc = MagicMock()
        mock_doc.metadata = {
            "song": "Lạc Trôi",
            "artist": "Sơn Tùng MTP",
            "total_plays": 1000,
            "unique_listeners": 500,
            "paid_ratio_pct": 40,
        }
        mock_ret = MagicMock()
        mock_ret.invoke.return_value = [mock_doc]
        mock_retriever.return_value = mock_ret

        result = recommend_personalized.invoke({"user_id": "user1", "query": ""})
        assert "Lạc Trôi" in result
        assert "Power User" in result


# -----------------------------------------------------------------------
# Mood Recommender Tests
# -----------------------------------------------------------------------
class TestRecommendByMood:
    """Tests for recommend_by_mood tool."""

    @patch("tools.mood_recommender._classify_mood")
    def test_ambiguous_mood(self, mock_classify):
        """Verify fallback when mood is unclear."""
        from tools.mood_recommender import recommend_by_mood

        mock_classify.return_value = None
        result = recommend_by_mood.invoke({"message": "xin chào"})
        assert "chưa rõ" in result.lower() or "mô tả" in result.lower()

    @patch("tools.mood_recommender.collection_exists")
    @patch("tools.mood_recommender._classify_mood")
    def test_rag_not_ready(self, mock_classify, mock_coll):
        """Verify message when RAG is not ready."""
        from tools.mood_recommender import recommend_by_mood

        mock_classify.return_value = "sad"
        mock_coll.return_value = False

        result = recommend_by_mood.invoke({"message": "tôi buồn"})
        assert "RAG" in result or "chưa sẵn sàng" in result

    @patch("tools.mood_recommender.get_retriever")
    @patch("tools.mood_recommender.collection_exists")
    @patch("tools.mood_recommender._classify_mood")
    def test_successful_mood_recommendation(self, mock_classify, mock_coll, mock_retriever):
        """Verify mood detection and recommendation."""
        from tools.mood_recommender import recommend_by_mood

        mock_classify.return_value = "sad"
        mock_coll.return_value = True

        mock_doc = MagicMock()
        mock_doc.metadata = {
            "song": "Hạ Trắng",
            "artist": "Trịnh Công Sơn",
            "total_plays": 800,
            "unique_listeners": 400,
            "paid_ratio_pct": 30,
        }
        mock_ret = MagicMock()
        mock_ret.invoke.return_value = [mock_doc]
        mock_retriever.return_value = mock_ret

        result = recommend_by_mood.invoke({"message": "tôi buồn"})
        assert "Buồn" in result
        assert "Hạ Trắng" in result


# -----------------------------------------------------------------------
# Mood Categories Test
# -----------------------------------------------------------------------
class TestMoodCategories:
    """Tests for mood categories and keyword mapping."""

    def test_all_moods_have_keywords(self):
        """Verify all mood categories have keywords defined."""
        from tools.mood_recommender import MOOD_DISPLAY, MOOD_KEYWORDS

        for mood in MOOD_KEYWORDS:
            assert len(MOOD_KEYWORDS[mood]) > 0
            assert mood in MOOD_DISPLAY

    def test_mood_count(self):
        """Verify 8 mood categories exist."""
        from tools.mood_recommender import MOOD_KEYWORDS

        assert len(MOOD_KEYWORDS) == 8


# -----------------------------------------------------------------------
# Playlist CRUD Tests
# -----------------------------------------------------------------------
class TestDeletePlaylist:
    """Tests for delete_playlist tool."""

    @patch("tools.playlist._get_bq_client")
    def test_playlist_not_found(self, mock_client):
        """Verify error when playlist doesn't exist."""
        from tools.playlist import delete_playlist

        mock_query = MagicMock()
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.cnt = 0
        mock_result.result.return_value = [mock_row]
        mock_query.query.return_value = mock_result
        mock_client.return_value = mock_query

        result = delete_playlist.invoke({"user_id": "user1", "playlist_name": "Nonexistent"})
        assert "Không tìm thấy" in result

    @patch("tools.playlist._get_bq_client")
    def test_successful_deletion(self, mock_client):
        """Verify successful playlist deletion."""
        from tools.playlist import delete_playlist

        mock_query = MagicMock()
        # First call: check exists (cnt=1)
        mock_check_result = MagicMock()
        mock_check_row = MagicMock()
        mock_check_row.cnt = 1
        mock_check_result.result.return_value = [mock_check_row]
        # Second call: delete
        mock_delete_result = MagicMock()
        mock_delete_result.result.return_value = []

        mock_query.query.side_effect = [
            mock_check_result,
            mock_delete_result,
        ]
        mock_client.return_value = mock_query

        result = delete_playlist.invoke({"user_id": "user1", "playlist_name": "Nhạc buồn"})
        assert "xóa" in result.lower() and "thành công" in result.lower()


class TestUpdatePlaylist:
    """Tests for update_playlist tool."""

    @patch("tools.playlist._get_bq_client")
    def test_playlist_not_found(self, mock_client):
        """Verify error when playlist doesn't exist."""
        from tools.playlist import update_playlist

        mock_query = MagicMock()
        mock_result = MagicMock()
        mock_result.result.return_value = []
        mock_query.query.return_value = mock_result
        mock_client.return_value = mock_query

        result = update_playlist.invoke(
            {
                "user_id": "user1",
                "playlist_name": "Nonexistent",
                "songs_to_add": "Song A",
            }
        )
        assert "Không tìm thấy" in result

    def test_empty_songs(self):
        """Verify error when no songs provided."""
        from tools.playlist import update_playlist

        result = update_playlist.invoke(
            {
                "user_id": "user1",
                "playlist_name": "My List",
                "songs_to_add": "  ,  ,  ",
            }
        )
        assert "Vui lòng cung cấp" in result


class TestRemoveSongFromPlaylist:
    """Tests for remove_song_from_playlist tool."""

    @patch("tools.playlist._get_bq_client")
    def test_playlist_not_found(self, mock_client):
        """Verify error when playlist doesn't exist."""
        from tools.playlist import remove_song_from_playlist

        mock_query = MagicMock()
        mock_result = MagicMock()
        mock_result.result.return_value = []
        mock_query.query.return_value = mock_result
        mock_client.return_value = mock_query

        result = remove_song_from_playlist.invoke(
            {
                "user_id": "user1",
                "playlist_name": "Nonexistent",
                "song_name": "Song A",
            }
        )
        assert "Không tìm thấy" in result

    @patch("tools.playlist._get_bq_client")
    def test_song_not_in_playlist(self, mock_client):
        """Verify error when song is not in playlist."""
        from tools.playlist import remove_song_from_playlist

        mock_query = MagicMock()
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.songs = ["Song A", "Song B"]
        mock_result.result.return_value = [mock_row]
        mock_query.query.return_value = mock_result
        mock_client.return_value = mock_query

        result = remove_song_from_playlist.invoke(
            {
                "user_id": "user1",
                "playlist_name": "My List",
                "song_name": "Song C",
            }
        )
        assert "Không tìm thấy bài" in result
