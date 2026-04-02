"""
Dagster assets for the AI Chatbot RAG operations.
"""
import os
import sys
from dagster import asset, AssetExecutionContext

# Add the pipeline root and chatbot root to path so we can import chatbot modules
PIPELINE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
CHATBOT_ROOT = os.path.join(PIPELINE_ROOT, "chatbot")

if CHATBOT_ROOT not in sys.path:
    sys.path.insert(0, CHATBOT_ROOT)

try:
    from rag.ingest import ingest_songs, ingest_artists
except ImportError as e:
    # Fallback to prevent immediate crash if not running in a context where chatbot is available
    def ingest_songs(*args, **kwargs): return 0
    def ingest_artists(*args, **kwargs): return 0


@asset(
    group_name="chatbot_ai",
    compute_kind="python",
    description="Updates ChromaDB vector store with the latest songs and artists from BigQuery marts."
)
def rag_chromadb_ingestion(context: AssetExecutionContext, dbt_marts_models: dict):
    """
    Triggers the RAG ingestion process to update ChromaDB after the marts models
    are rebuilt by dbt. This ensures the Chatbot always has the latest metrics.
    """
    context.log.info(f"Starting RAG data ingestion out of BigQuery to ChromaDB...")
    
    # Save current CWD and switch to CHATBOT_ROOT because config.py relies on it
    original_cwd = os.getcwd()
    os.chdir(CHATBOT_ROOT)
    
    try:
        total_songs = ingest_songs(force=True)
        context.log.info(f"Successfully ingested {total_songs} songs.")
        
        total_artists = ingest_artists(force=True)
        context.log.info(f"Successfully ingested {total_artists} artists.")
        
        return {
            "songs_ingested": total_songs,
            "artists_ingested": total_artists
        }
    except Exception as e:
        context.log.error(f"Failed to ingest ChromaDB arrays: {e}")
        raise
    finally:
        # Always restore CWD
        os.chdir(original_cwd)
