# 🎵 SoundFlow - Real-time Music Streaming Analytics Pipeline

## 📋 Table of Contents

1. [Overview](#-overview)
2. [System Architecture](#-system-architecture)
3. [Prerequisites](#-prerequisites)
4. [Deployment Guide](#-deployment-guide)
5. [Project Structure](#-project-structure)
6. [Running the Pipeline](#-running-the-pipeline)
7. [Data Models](#-data-models)
8. [Troubleshooting](#-troubleshooting)

---

## 🎯 Overview

SoundFlow is an end-to-end data pipeline that processes streaming data from a simulated music streaming application. The pipeline collects user events, stores them in a Data Lake (GCS), and transforms them into analytics tables in BigQuery.

### Event Types

| Event Type | Description | Count (50K test) |
|------------|-------------|------------------|
| `listen_events` | User plays a song | 24,250 |
| `page_view_events` | User views a page | 29,335 |
| `auth_events` | Login/logout | 435 |
| `status_change_events` | Subscription changes | 21 |

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         SoundFlow Data Pipeline                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  LOCAL ENVIRONMENT                           GOOGLE CLOUD PLATFORM              │
│  ──────────────────                          ────────────────────               │
│                                                                                  │
│  ┌──────────────┐    ┌──────────────┐       ┌────────────────────────────────┐ │
│  │   EventSim   │───▶│   Redpanda   │       │       Cloud Storage (GCS)      │ │
│  │   (Docker)   │    │   (Kafka)    │       │  gs://bucket/raw/              │ │
│  │              │    │   :9092      │       │  ├── listen_events/            │ │
│  │  Scala app   │    │              │       │  │   └── year=/month=/day=/    │ │
│  │  generates   │    │  4 topics:   │       │  ├── page_view_events/         │ │
│  │  user events │    │  - listen    │       │  ├── auth_events/              │ │
│  └──────────────┘    │  - page_view │       │  └── status_change_events/     │ │
│                      │  - auth      │       │           ▲                     │ │
│                      │  - status    │       │           │ Parquet files       │ │
│                      └──────┬───────┘       │           │ (partitioned)       │ │
│                             │               └───────────┼────────────────────┘ │
│                             │                           │                       │
│                             ▼                           │                       │
│                      ┌──────────────┐                   │                       │
│                      │    Python    │───────────────────┘                       │
│                      │    Script    │                                           │
│                      │              │       ┌────────────────────────────────┐ │
│                      │ kafka_to_gcs │       │         BigQuery               │ │
│                      │ _python.py   │       │                                │ │
│                      │              │       │  raw (External Tables)         │ │
│                      │ • confluent  │       │  ├── ext_listen_events         │ │
│                      │   -kafka     │       │  ├── ext_page_view_events      │ │
│                      │ • pyarrow    │       │  ├── ext_auth_events           │ │
│                      │ • gcs client │       │  └── ext_status_change_events  │ │
│                      └──────────────┘       │           │                     │ │
│                                             │           ▼                     │ │
│                                             │  staging (Views)                │ │
│                      ┌──────────────┐       │  ├── stg_listens                │ │
│                      │     dbt      │──────▶│  ├── stg_page_views             │ │
│                      │              │       │  ├── stg_auth                   │ │
│                      │ 13 models    │       │  └── stg_status_changes         │ │
│                      │ 31 tests     │       │           │                     │ │
│                      │              │       │           ▼                     │ │
│                      └──────────────┘       │  intermediate (Views)           │ │
│                                             │  ├── int_daily_metrics          │ │
│                                             │  ├── int_song_stats             │ │
│                                             │  └── int_user_activity          │ │
│                                             │           │                     │ │
│                                             │           ▼                     │ │
│                                             │  marts (Tables)                 │ │
│                                             │  ├── mart_hourly_metrics        │ │
│                                             │  ├── mart_daily_summary         │ │
│                                             │  ├── mart_location_analytics    │ │
│                                             │  ├── mart_active_users          │ │
│                                             │  ├── mart_top_songs             │ │
│                                             │  └── mart_top_artists           │ │
│                                             └────────────────────────────────┘ │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Data Generator | EventSim (Scala) | Simulates user behavior |
| Message Queue | Redpanda (Kafka-compatible) | Buffers streaming events |
| ETL Script | Python (confluent-kafka, pyarrow) | Kafka → GCS transfer |
| Data Lake | Google Cloud Storage | Raw data storage (Parquet) |
| Data Warehouse | BigQuery | Analytics & transformations |
| Transformation | dbt | Data modeling & testing |
| Infrastructure | Terraform | Infrastructure as Code |

---

## 📦 Prerequisites

### Required Software

| Tool | Version | Purpose |
|------|---------|---------|
| Docker Desktop | Latest | Run Redpanda & EventSim |
| Python | >= 3.10 | ETL script & dbt |
| Google Cloud SDK | Latest | Interact with GCP |
| Terraform | >= 1.0 | Deploy infrastructure |

### Python Packages

```bash
pip install -r requirements.txt
```

Main packages:
- `confluent-kafka` - Kafka consumer
- `pyarrow` - Parquet file handling
- `pandas` - Data manipulation
- `google-cloud-storage` - GCS upload
- `dbt-bigquery` - Data transformation

### GCP Setup

1. Create GCP Project
2. Enable APIs:
   - Cloud Storage API
   - BigQuery API
   
3. Create Service Account with roles:
   - `roles/storage.objectAdmin`
   - `roles/bigquery.dataEditor`
   - `roles/bigquery.jobUser`

4. Download JSON key and save to `credentials/`

---

## 🚀 Deployment Guide

### Step 1: Clone and Configure

```powershell
# Clone repository
git clone <repo-url>
cd Data_streaming_pipeline

# Create virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Configure Credentials

```powershell
# Copy service account key
mkdir credentials
# Place JSON key file at credentials/dbt-sa-key.json

# Configure .env
cp .env.example .env
# Edit values in .env
```

### Step 3: Deploy GCP Infrastructure (Terraform)

```powershell
cd terraform

# Initialize
terraform init

# Review plan
terraform plan

# Apply
terraform apply -auto-approve

# Enable external tables (after data exists)
terraform apply -var="enable_external_tables=true" -auto-approve
```

### Step 4: Configure dbt

```powershell
cd dbt

# Test connection
dbt debug --target prod

# Install packages
dbt deps
```

---

## 📁 Project Structure

```
Data_streaming_pipeline/
│
├── 📂 eventsim/                    # Data Generator
│   ├── docker/
│   │   └── Dockerfile
│   ├── examples/
│   │   └── example-config.json     # EventSim config
│   └── src/main/scala/             # Scala source code
│
├── 📂 redpanda/                    # Message Queue (Kafka)
│   └── docker-compose.yml
│
├── 📂 spark_streaming/             # ETL Scripts
│   └── src/
│       └── kafka_to_gcs_python.py  # Main ETL script
│
├── 📂 dbt/                         # Data Transformation
│   ├── models/
│   │   ├── staging/                # stg_* views
│   │   ├── intermediate/           # int_* views
│   │   └── marts/                  # mart_* tables
│   ├── macros/
│   ├── tests/
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── 📂 terraform/                   # Infrastructure as Code
│   ├── main.tf
│   ├── variables.tf
│   ├── gcs.tf
│   ├── bigquery.tf
│   └── iam.tf
│
├── 📂 dagster/                     # Orchestration (future)
│
├── 📂 credentials/                 # Service account keys (gitignored)
│   └── dbt-sa-key.json
│
├── 📂 docs/                        # Documentation
│
├── .env                            # Environment variables
├── docker-compose.yml              # Main docker compose
└── requirements.txt                # Python dependencies
```

---

## ▶️ Running the Pipeline

### Quick Start (Full Pipeline)

```powershell
# 1. Start Redpanda (Kafka)
docker-compose up -d redpanda

# 2. Generate events with EventSim
docker-compose up eventsim

# 3. Transfer data from Kafka → GCS
cd spark_streaming/src
python kafka_to_gcs_python.py

# 4. Run dbt transformations
cd ../../dbt
dbt run --target prod --full-refresh

# 5. Run dbt tests
dbt test --target prod
```

### Detailed Steps

#### 1️⃣ Start Redpanda

```powershell
docker-compose up -d redpanda

# Verify
docker-compose ps
# Redpanda should be healthy on port 9092
```

#### 2️⃣ Generate Events (EventSim)

Configure event count in `.env`:
```env
EVENTSIM_NUSERS=500      # Number of simulated users
EVENTSIM_FROM=30         # Start from 30 days ago
EVENTSIM_TO=0            # End at today
```

```powershell
# Run eventsim (not continuous - runs once)
docker-compose up eventsim

# Or run in continuous mode
docker-compose run eventsim --continuous
```

#### 3️⃣ Upload to GCS

```powershell
cd spark_streaming/src
python kafka_to_gcs_python.py
```

Expected output:
```
✓ Processed 24250 records for listen_events
✓ Processed 29335 records for page_view_events  
✓ Processed 435 records for auth_events
✓ Processed 21 records for status_change_events
```

#### 4️⃣ Run dbt

```powershell
cd dbt

# Full refresh (recreate all models)
dbt run --target prod --full-refresh

# Incremental run
dbt run --target prod

# Run specific model
dbt run --target prod --select mart_top_songs
```

#### 5️⃣ Run Tests

```powershell
dbt test --target prod
```

Expected: `PASS=31 ERROR=0`

---

## 📊 Data Models

### Staging Layer (Views)

| Model | Source | Description |
|-------|--------|-------------|
| `stg_listens` | ext_listen_events | Cleaned listen events |
| `stg_page_views` | ext_page_view_events | Cleaned page views |
| `stg_auth` | ext_auth_events | Cleaned auth events |
| `stg_status_changes` | ext_status_change_events | Cleaned status changes |

### Intermediate Layer (Views)

| Model | Description |
|-------|-------------|
| `int_daily_metrics` | Daily aggregated metrics |
| `int_song_stats` | Song-level statistics |
| `int_user_activity` | User activity summary |

### Marts Layer (Tables)

| Model | Description | Key Metrics |
|-------|-------------|-------------|
| `mart_hourly_metrics` | Hourly KPIs | listens, users, sessions per hour |
| `mart_daily_summary` | Daily overview | total listens, unique users, top songs |
| `mart_location_analytics` | Geographic analysis | listens by city/state |
| `mart_active_users` | User engagement | listen count, engagement tier |
| `mart_top_songs` | Top 100 songs | play count, unique listeners |
| `mart_top_artists` | Top 100 artists | play count, song count |

### Lineage

```
External Tables (raw)
       │
       ▼
   Staging Views ──────────────────────────┐
       │                                    │
       ▼                                    │
 Intermediate Views                         │
       │                                    │
       ▼                                    ▼
   Marts Tables ◄──────────────────────────┘
```

---

## 🔧 Troubleshooting

### Common Errors

#### 1. Kafka connection refused
```
Error: Connection to localhost:9092 refused
```
**Solution**: Check if Redpanda is running
```powershell
docker-compose ps
docker-compose up -d redpanda
```

#### 2. GCS permission denied
```
Error: 403 Forbidden
```
**Solution**: Verify service account has `storage.objectAdmin` role
```powershell
gcloud storage buckets add-iam-policy-binding gs://YOUR_BUCKET \
  --member="serviceAccount:YOUR_SA@PROJECT.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

#### 3. BigQuery schema mismatch
```
Error: Parquet column 'X' has type BYTE_ARRAY which does not match INT64
```
**Solution**: Delete and recreate external table
```powershell
bq rm -f -t PROJECT:raw.ext_listen_events
# Recreate with autodetect
```

#### 4. dbt model not found
```
Error: Relation does not exist
```
**Solution**: Run with `--full-refresh`
```powershell
dbt run --target prod --full-refresh
```

### Useful Commands

```powershell
# Check Kafka topics
docker exec -it redpanda rpk topic list

# Check topic messages
docker exec -it redpanda rpk topic consume listen_events -n 5

# Check GCS files
gsutil ls -r gs://YOUR_BUCKET/raw/

# Check BigQuery table schema
bq show --format=prettyjson PROJECT:raw.ext_listen_events

# dbt debug
dbt debug --target prod
```

---

## 📞 Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [BigQuery External Tables](https://cloud.google.com/bigquery/docs/external-tables)
- [Redpanda Documentation](https://docs.redpanda.com/)
- [Terraform GCP Provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)

---

## 📝 License

MIT License
