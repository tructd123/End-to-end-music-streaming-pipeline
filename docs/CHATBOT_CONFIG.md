# 🤖 Chatbot Module Configuration

## Overview

The SoundFlow chatbot is an **AI Agent** built with **LangGraph** using the **ReAct pattern**. It utilizes **Google Gemini** as the main LLM to intelligently route queries and **ChromaDB** as the robust vector store for Retrieval-Augmented Generation (RAG) based song recommendations. The Chatbot exposes API endpoints via **FastAPI** and is equipped with a user-friendly frontend allowing authentication simulation and real-time streaming capabilities.

**Default language**: Vietnamese (Answer in English unless otherwise specified).

## 🏗 Architecture

```text
User Request
     │
     ▼
FastAPI Server (port 8000)
     │
     ▼
LangGraph Agent (ReAct Pattern)
     │
     ├─→ chatbot_node (Gemini LLM)
     │        │
     │        ├─ No tool calls → Return Response to User (END)
     │        └─ Has tool calls → tools_node → trigger corresponding Data tool
     │
     └─→ Custom Tools:
          ├── recommend_songs     → ChromaDB RAG (Deep search in BQ Marts)
          ├── search_songs        → BigQuery mart_top_songs
          ├── search_artists      → BigQuery mart_top_artists
          ├── get_user_stats      → BigQuery mart_active_users
          ├── change_subscription → BigQuery mart_active_users
          └── playlist            → Manage User's playlists
```

## 📁 File Structure

The chatbot codebase lives inside the `chatbot/` directory:

```text
chatbot/
├── app.py              # FastAPI server (endpoints: /health, /chat/stream)
├── config.py           # Defines Settings (loads from .env)
├── Dockerfile          # Multi-stage Docker build config
├── requirements.txt    # Python dependencies
├── pytest.ini          # Pytest setup and coverage
├── tests/              # 33+ Pytest cases for memory, API, tools, streaming
│
├── agent/              # The LangGraph Logic
│   ├── graph.py        # StateGraph routing
│   ├── prompts.py      # Core SoundFlow System Prompt
│   ├── state.py        # AgentState definition
│   └── streaming.py    # Async streaming logic generator
│
├── rag/                # RAG (Retrieval-Augmented Generation) Pipeline
│   ├── ingest.py       # BigQuery → ChromaDB data digestion script
│   ├── retriever.py    # Similarity search configurations 
│   └── vectorstore.py  # ChromaDB Collections via Gemini Embeddings
│
├── tools/              # Specialized LangChain @tools functions
│   ├── playlist.py
│   ├── search.py
│   ├── song_recommender.py
│   ├── subscription.py
│   └── user_stats.py
│
└── static/             # Frontend UI
    ├── app.js
    ├── index.html      # UI Interface (Dropdown Auth, Chat Box)
    └── styles.css
```

## ⚙️ Environment Setup (`chatbot/.env`)

Create a local `.env` inside the `chatbot/` folder based on `.env.example`:

| Variable | Recommended Default | Description |
|---|---|---|
| `GEMINI_API_KEY` | *(required)* | Set your personal Gemini App Key. |
| `LLM_MODEL` | `gemini-2.5-flash` | Core Chat model to invoke. |
| `EMBEDDING_MODEL` | `models/gemini-embedding-001` | Dedicated Embeddings logic for Chroma vectors. |
| `GCP_PROJECT` | *(required)* | Your GCP BigQuery Project ID. |
| `GOOGLE_APPLICATION_CREDENTIALS` | `../credentials/pipeline-sa-key.json` | Path to valid BigQuery GCP Service account logic. |
| `BQ_DATASET_MARTS` | `marts` | Dedicated BQ dataset. |
| `CHROMA_PERSIST_DIR` | `./chroma_data` | Persistent path for Sqlite vector storage. |
| `API_HOST` | `0.0.0.0` | Default FastAPI address |
| `API_PORT` | `8000` | Default listening port |

---

## 🚀 Running the Chatbot

### Running Locally (Native Python)
If you wish to spin it up native locally without Docker:

```bash
cd chatbot

# Enable environment context
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Start FastAPI and Uvicorn
uvicorn app:app --reload --port 8000
```
Open `http://localhost:8000/` in your browser to interact with the UI.

### Ingest Data (RAG Setup)
For the Semantic `recommend_songs` tool to work, you must sync BigQuery data down into your ChromaDB space:
```bash
cd chatbot
python -m rag.ingest
```

### Running via Docker Compose
The Chatbot is already configured within the main `docker-compose.yml` under the `chatbot` service (with volume binding map to `chatbot-chroma`).

```bash
cd Data_streaming_pipeline
docker compose up chatbot -d
```
Your Chatbot will boot in the multi-stage docker environment and listen at `http://localhost:8000`.

---

## 🧪 Testing

The chatbot ecosystem features comprehensive Test-Driven Development (TDD) via `pytest`. This includes rigorous test suites for memory routing, fallback constraints, and health checking endpoints.

```bash
cd chatbot
pytest tests/ -v
```

### CI/CD Pipeline
Continuous Integration and Delivery is fully managed via **GitHub Actions** (at `.github/workflows/chatbot-ci.yml`):
- **Lint**: Setup Python >= 3.11, validates formatting with `ruff format --check` and `ruff check`.
- **Test**: Executes testing and limits Coverage thresholds (`pytest --cov-fail-under=70`).
- **Security Check**: Enforces Security validations using `Snyk` dependency scanner.
- **Build**: Generates the final docker image upon branch merge/push completions.