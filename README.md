# 🎵 SoundFlow

Real-time data pipeline for music streaming analytics, simulating a platform like Spotify.

## 📋 Objective

Stream events from a fake music streaming service and build a pipeline to process real-time data. Data is periodically stored in a data lake, then transformed via dbt to create analytics dashboards.

**Analytics Metrics:** Popular songs, Active users, User demographics, Listening patterns.

## 📦 Tech Stack

| Component | Technology |
|-----------|------------|
| Data Generator | EventSim (Scala) |
| Message Broker | Redpanda (Kafka-compatible) |
| Data Lake | Google Cloud Storage |
| Data Warehouse | BigQuery |
| Transformation | dbt |
| Orchestration | Dagster |
| Infrastructure | Terraform |
| Containerization | Docker |
| Dashboard | Looker Studio / Metabase |

## 🏗️ Architecture

```
EventSim ──▶ Redpanda ──▶ Python ETL ──▶ GCS (Parquet)
                                              │
                                              ▼
                              BigQuery (External Tables)
                                              │
                                              ▼
                              dbt (staging → marts)
                                              │
                                              ▼
                                    Dashboard
```

## 🚀 Quick Start

### Prerequisites

- Docker Desktop
- Python >= 3.10  
- GCP Account + Service Account key
- Terraform >= 1.0

### 1. Setup

```bash
git clone <repo-url>
cd Data_streaming_pipeline

python -m venv .venv
.venv\Scripts\Activate.ps1   # Windows
pip install -r requirements.txt

cp .env.example .env
# Edit .env with your GCP settings
```

### 2. Infrastructure

```bash
cd terraform
terraform init
terraform apply
```

### 3. Run Pipeline

```bash
# Start all services
docker compose up -d

# Access Dagster UI: http://localhost:3000
# Access Redpanda Console: http://localhost:8080
```

> 📖 Details: [docs/QUICKSTART.md](docs/QUICKSTART.md)

## 📁 Project Structure

```
├── dagster/          # Orchestration (assets, schedules)
├── dbt/              # Transformations (13 models, 31 tests)
├── eventsim/         # Event generator config
├── redpanda/         # Kafka broker config
├── spark_streaming/  # ETL scripts
├── terraform/        # GCP infrastructure
├── dashboard/        # Looker Studio / Metabase setup
├── scripts/          # Utility scripts
└── docs/             # Documentation
```

## 📊 Data Models

| Mart | Description |
|------|-------------|
| `mart_daily_summary` | Daily KPIs |
| `mart_hourly_metrics` | Hourly metrics |
| `mart_active_users` | User engagement |
| `mart_top_songs` | Top 100 songs |
| `mart_top_artists` | Top 100 artists |
| `mart_location_analytics` | Geographic analytics |

> 📖 Details: [dashboard/looker_studio/DATA_DICTIONARY.md](dashboard/looker_studio/DATA_DICTIONARY.md)

## 🐳 Docker Services

| Service | Port | Description |
|---------|------|-------------|
| Redpanda | 9092 | Kafka broker |
| Redpanda Console | 8080 | Kafka UI |
| Dagster | 3000 | Orchestration UI |
| Metabase | 3030 | Dashboard (optional) |

```bash
# GCP Mode (default)
docker compose up -d

# Local Mode (with Postgres)
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d

# With Dashboard
docker compose --profile dashboard up -d
```

## 📚 Documentation

| Doc | Content |
|-----|---------|
| [QUICKSTART.md](docs/QUICKSTART.md) | Quick start guide |
| [GCP_CONFIG.md](docs/GCP_CONFIG.md) | GCP configuration |
| [DAGSTER_GUIDE.md](docs/DAGSTER_GUIDE.md) | Using Dagster |
| [GCP_UI_GUIDE.md](docs/GCP_UI_GUIDE.md) | GCP Console guide |

## ⚠️ Notes

- **GCP Costs**: Use Free Tier or $300 credit for new accounts
- **Credentials**: Do not commit `.json` credential files
- **Cleanup**: Run `terraform destroy` when not in use

## 🔧 Troubleshooting

```bash
# Check environment
python scripts/check_env.py

# View logs
docker compose logs -f dagster-webserver
```

## 📈 Future Improvements

- [ ] Managed Kafka (Confluent Cloud)
- [ ] Cloud Composer for orchestration
- [ ] Data quality monitoring
- [ ] Real-time dashboard
...

## 🔄 CI/CD

GitHub Actions workflow for dbt:

| Trigger | Actions |
|---------|---------|
| PR to `main` | Lint → Test (dev) |
| Push to `main` | Lint → Test → Deploy (prod) |

> 📖 Setup: [.github/workflows/README.md](.github/workflows/README.md)

---

**Credits**: Based on [Streamify](https://github.com/ankurchavda/streamify) and [DataTalks.Club](https://datatalks.club) [DE Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp).
