# 🎵 SoundFlow - Music Streaming Data Pipeline

End-to-end data pipeline for music streaming analytics, simulating a Spotify-like platform.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         SoundFlow Data Pipeline                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  LOCAL                                       GOOGLE CLOUD PLATFORM              │
│                                                                                  │
│  ┌──────────────┐    ┌──────────────┐       ┌────────────────────────────────┐ │
│  │   EventSim   │───▶│   Redpanda   │       │       Cloud Storage (GCS)      │ │
│  │   (Docker)   │    │   (Kafka)    │       │  gs://bucket/raw/              │ │
│  │              │    │   :9092      │       │  ├── listen_events/            │ │
│  │  Generates   │    │              │       │  │   └── year=/month=/day=/    │ │
│  │  user events │    │  4 topics    │       │  ├── page_view_events/         │ │
│  └──────────────┘    └──────┬───────┘       │  ├── auth_events/              │ │
│                             │               │  └── status_change_events/     │ │
│                             │               └───────────────┬────────────────┘ │
│                             ▼                               │                   │
│                      ┌──────────────┐                       │                   │
│                      │    Python    │───────────────────────┘                   │
│                      │    Script    │         Parquet files                     │
│                      │              │         (partitioned)                     │
│                      │ kafka_to_gcs │                                           │
│                      │ _python.py   │       ┌────────────────────────────────┐ │
│                      └──────────────┘       │         BigQuery               │ │
│                                             │                                │ │
│                                             │  raw (External Tables)         │ │
│                      ┌──────────────┐       │  ├── ext_listen_events         │ │
│                      │     dbt      │──────▶│  ├── ext_page_view_events      │ │
│                      │              │       │  ├── ext_auth_events           │ │
│                      │ 13 models    │       │  └── ext_status_change_events  │ │
│                      │ 31 tests     │       │           │                     │ │
│                      └──────────────┘       │           ▼                     │ │
│                                             │  staging ──▶ intermediate       │ │
│                                             │           │                     │ │
│                                             │           ▼                     │ │
│                                             │        marts                    │ │
│                                             └────────────────────────────────┘ │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 📦 Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **EventSim** | Scala/Docker | Simulates music streaming events |
| **Redpanda** | Kafka-compatible | Message broker (port 9092) |
| **ETL Script** | Python | Kafka → GCS transfer |
| **Data Lake** | Google Cloud Storage | Raw data storage (Parquet) |
| **Data Warehouse** | BigQuery | Analytics & transformations |
| **Transformation** | dbt | Data modeling & testing |
| **Infrastructure** | Terraform | Infrastructure as Code |

## 🚀 Quick Start

### Prerequisites

- Docker Desktop
- Python >= 3.10
- GCP Account with credentials
- Terraform >= 1.0

### Step 1: Setup Environment

```bash
# Clone repository
git clone <repo-url>
cd Data_streaming_pipeline

# Create virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1  # Windows
# source .venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your settings
```

### Step 2: Deploy GCP Infrastructure

```bash
cd terraform

# Initialize and apply
terraform init
terraform apply -auto-approve

# Enable external tables (after data exists)
terraform apply -var="enable_external_tables=true" -auto-approve
```

### Step 3: Run the Pipeline

```bash
# 1. Start Kafka broker
docker-compose up -d redpanda

# 2. Generate events (~50K)
docker-compose up eventsim

# 3. Upload to GCS
cd spark_streaming/src
python kafka_to_gcs_python.py

# 4. Run dbt transformations
cd ../../dbt
dbt run --target prod --full-refresh
dbt test --target prod
```

## 📊 Data Models

### Events Generated

| Event Type | Description | Example Volume |
|------------|-------------|----------------|
| `listen_events` | User plays a song | 24,250 |
| `page_view_events` | User views a page | 29,335 |
| `auth_events` | Login/logout | 435 |
| `status_change_events` | Subscription changes | 21 |

### dbt Model Layers

```
External Tables (raw)
       │
       ▼
┌─────────────────┐
│  Staging Views  │  stg_listens, stg_page_views, stg_auth, stg_status_changes
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Intermediate    │  int_daily_metrics, int_song_stats, int_user_activity
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Marts Tables   │  Analytics-ready tables
└─────────────────┘
```

### Analytics Marts

| Mart | Description | Key Metrics |
|------|-------------|-------------|
| `mart_hourly_metrics` | Hourly KPIs | Listens, users, sessions per hour |
| `mart_daily_summary` | Daily overview | Total listens, unique users |
| `mart_location_analytics` | Geographic analysis | Listens by city/state |
| `mart_active_users` | User engagement | Listen count, engagement tier |
| `mart_top_songs` | Top 100 songs | Play count, unique listeners |
| `mart_top_artists` | Top 100 artists | Play count, song count |

## 📁 Project Structure

```
Data_streaming_pipeline/
├── eventsim/                   # Event generator (Scala)
│   ├── docker/
│   └── examples/
├── redpanda/                   # Kafka broker config
├── spark_streaming/            # ETL scripts
│   └── src/
│       └── kafka_to_gcs_python.py
├── dbt/                        # Transformations
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── profiles.yml
├── terraform/                  # Infrastructure as Code
│   ├── gcs.tf
│   ├── bigquery.tf
│   └── iam.tf
├── dagster/                    # Orchestration (optional)
├── credentials/                # GCP keys (gitignored)
├── docs/                       # Documentation
├── docker-compose.yml
├── requirements.txt
└── .env
```

## 🔧 Configuration

### Environment Variables (.env)

```env
# GCP
GCP_PROJECT_ID=your-project-id
GCP_REGION=asia-southeast1
GCS_BUCKET=your-bucket-name
GOOGLE_APPLICATION_CREDENTIALS=./credentials/dbt-sa-key.json

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# EventSim
EVENTSIM_NUSERS=500      # Number of simulated users
EVENTSIM_FROM=30         # Start from N days ago
EVENTSIM_TO=0            # End at today
```

### Terraform Variables

```hcl
project_id              = "your-gcp-project"
region                  = "asia-southeast1"
environment             = "dev"
enable_external_tables  = true
```

## 💰 Cost Estimation

| Resource | Usage | Est. Monthly Cost |
|----------|-------|-------------------|
| GCS Storage | 10 GB | ~$0.20 |
| BigQuery Storage | 50 GB | ~$1.00 |
| BigQuery Queries | 1 TB/month | ~$5.00 |
| **Total** | | **~$6/month** |

*With 50K events (~5MB), costs stay within GCP free tier.*

## 🧪 Test Results

Pipeline tested with 54,041 events:

| Component | Status |
|-----------|--------|
| Events Generated | ✅ 54,041 |
| GCS Upload | ✅ 4 topics |
| dbt Models | ✅ 13/13 passed |
| dbt Tests | ✅ 31/31 passed |

## 📚 Documentation

- [Quick Start Guide](docs/QUICKSTART.md)
- [GCP Configuration](docs/GCP_CONFIG.md)
- [Full Documentation](docs/README.md)

## 🛠️ Development

### Run dbt locally

```bash
cd dbt

# Test connection
dbt debug --target prod

# Run models
dbt run --target prod
dbt test --target prod

# Generate docs
dbt docs generate
dbt docs serve
```

### Useful Commands

```bash
# Check Kafka topics
docker exec -it redpanda rpk topic list

# Query BigQuery
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM raw.ext_listen_events"

# View GCS files
gsutil ls -r gs://YOUR_BUCKET/raw/
```

## 📝 License

MIT
