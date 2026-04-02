#!/bin/bash

# Ingest data to ChromaDB on startup
echo "Running data ingestion to ChromaDB..."
python -m rag.ingest

# Start the FastAPI server
echo "Starting Uvicorn server..."
exec uvicorn app:app --host 0.0.0.0 --port 8000
