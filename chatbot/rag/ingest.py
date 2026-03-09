"""
SoundFlow AI Chatbot - Data Ingestion Script

Loads song and artist data from BigQuery into ChromaDB vector store
for RAG-based recommendations.

Usage:
    python -m rag.ingest
"""

import sys
from google.cloud import bigquery
from langchain_core.documents import Document

from config import settings
from rag.vectorstore import get_vectorstore, collection_exists


def fetch_top_songs(client: bigquery.Client) -> list[dict]:
    """Fetch top songs data from BigQuery mart_top_songs."""
    query = f"""
    SELECT
        rank,
        song,
        artist,
        total_plays,
        unique_listeners,
        unique_sessions,
        paid_plays,
        free_plays,
        paid_ratio_pct,
        days_with_plays,
        avg_plays_per_day,
        peak_time_of_day,
        plays_per_listener
    FROM `{settings.GCP_PROJECT}.{settings.BQ_DATASET_MARTS}.mart_top_songs`
    ORDER BY total_plays DESC
    """
    return [dict(row) for row in client.query(query).result()]


def fetch_top_artists(client: bigquery.Client) -> list[dict]:
    """Fetch top artists data from BigQuery mart_top_artists."""
    query = f"""
    SELECT
        rank,
        artist,
        total_songs,
        total_plays,
        total_listeners,
        paid_plays,
        free_plays,
        avg_plays_per_song,
        plays_per_listener,
        paid_ratio_pct
    FROM `{settings.GCP_PROJECT}.{settings.BQ_DATASET_MARTS}.mart_top_artists`
    ORDER BY total_plays DESC
    """
    return [dict(row) for row in client.query(query).result()]


def create_song_documents(songs: list[dict]) -> list[Document]:
    """Convert song data into LangChain Documents for embedding.

    Each document contains a text representation of the song
    and metadata for filtering and display.
    """
    documents = []
    for song in songs:
        # Create a rich text description for semantic search
        text = (
            f"Bài hát '{song['song']}' của nghệ sĩ {song['artist']}. "
            f"Đây là bài hát xếp hạng #{song['rank']} với {song['total_plays']} lượt nghe, "
            f"{song['unique_listeners']} người nghe khác nhau. "
            f"Tỉ lệ người nghe trả phí: {song['paid_ratio_pct']}%. "
            f"Thời điểm nghe phổ biến nhất: {song['peak_time_of_day']}. "
            f"Trung bình {song['avg_plays_per_day']} lượt nghe mỗi ngày."
        )

        metadata = {
            "type": "song",
            "rank": song["rank"],
            "song": song["song"],
            "artist": song["artist"],
            "total_plays": song["total_plays"],
            "unique_listeners": song["unique_listeners"],
            "paid_ratio_pct": float(song["paid_ratio_pct"] or 0),
            "peak_time_of_day": song["peak_time_of_day"],
        }

        documents.append(Document(page_content=text, metadata=metadata))

    return documents


def create_artist_documents(artists: list[dict]) -> list[Document]:
    """Convert artist data into LangChain Documents for embedding."""
    documents = []
    for artist in artists:
        text = (
            f"Nghệ sĩ {artist['artist']} có {artist['total_songs']} bài hát "
            f"với tổng cộng {artist['total_plays']} lượt nghe và "
            f"{artist['total_listeners']} người nghe. "
            f"Mỗi bài hát trung bình được nghe {artist['avg_plays_per_song']} lần. "
            f"Tỉ lệ người nghe trả phí: {artist['paid_ratio_pct']}%."
        )

        metadata = {
            "type": "artist",
            "rank": artist["rank"],
            "artist": artist["artist"],
            "total_songs": artist["total_songs"],
            "total_plays": artist["total_plays"],
            "total_listeners": artist["total_listeners"],
            "paid_ratio_pct": float(artist["paid_ratio_pct"] or 0),
        }

        documents.append(Document(page_content=text, metadata=metadata))

    return documents


def ingest_songs(force: bool = False) -> int:
    """Ingest song data from BigQuery into ChromaDB.

    Args:
        force: If True, re-ingest even if collection already has data.

    Returns:
        Number of documents ingested.
    """
    collection_name = settings.CHROMA_COLLECTION_SONGS

    if not force and collection_exists(collection_name):
        print(f"✅ Collection '{collection_name}' already has data. Use --force to re-ingest.")
        return 0

    print(f"📥 Fetching songs from BigQuery...")
    client = bigquery.Client(project=settings.GCP_PROJECT)
    songs = fetch_top_songs(client)
    print(f"   Found {len(songs)} songs")

    print(f"📝 Creating documents...")
    documents = create_song_documents(songs)

    print(f"🔄 Ingesting into ChromaDB collection '{collection_name}'...")
    vectorstore = get_vectorstore(collection_name)
    vectorstore.add_documents(documents)

    print(f"✅ Successfully ingested {len(documents)} songs!")
    return len(documents)


def ingest_artists(force: bool = False) -> int:
    """Ingest artist data from BigQuery into ChromaDB.

    Args:
        force: If True, re-ingest even if collection already has data.

    Returns:
        Number of documents ingested.
    """
    collection_name = settings.CHROMA_COLLECTION_ARTISTS

    if not force and collection_exists(collection_name):
        print(f"✅ Collection '{collection_name}' already has data. Use --force to re-ingest.")
        return 0

    print(f"📥 Fetching artists from BigQuery...")
    client = bigquery.Client(project=settings.GCP_PROJECT)
    artists = fetch_top_artists(client)
    print(f"   Found {len(artists)} artists")

    print(f"📝 Creating documents...")
    documents = create_artist_documents(artists)

    print(f"🔄 Ingesting into ChromaDB collection '{collection_name}'...")
    vectorstore = get_vectorstore(collection_name)
    vectorstore.add_documents(documents)

    print(f"✅ Successfully ingested {len(documents)} artists!")
    return len(documents)


def main():
    """Main entry point for data ingestion."""
    print("=" * 60)
    print("🎵 SoundFlow - Data Ingestion to ChromaDB")
    print("=" * 60)

    # Check required settings
    missing = settings.validate()
    if missing:
        print(f"\n❌ Missing required environment variables: {', '.join(missing)}")
        print("   Please set them in your .env file.")
        sys.exit(1)

    force = "--force" in sys.argv

    print(f"\n📂 ChromaDB directory: {settings.CHROMA_PERSIST_DIR}")
    print(f"📊 BigQuery project: {settings.GCP_PROJECT}")
    print(f"📦 BigQuery dataset: {settings.BQ_DATASET_MARTS}")
    print()

    # Ingest songs
    total_songs = ingest_songs(force=force)

    print()

    # Ingest artists
    total_artists = ingest_artists(force=force)

    print()
    print("=" * 60)
    print(f"🎉 Ingestion complete! Songs: {total_songs}, Artists: {total_artists}")
    print("=" * 60)


if __name__ == "__main__":
    main()
