# 🎵 SoundFlow - Real-time Music Streaming Analytics Pipeline

## 📋 Mục lục

1. [Tổng quan](#-tổng-quan)
2. [Kiến trúc hệ thống](#-kiến-trúc-hệ-thống)
3. [Yêu cầu cài đặt](#-yêu-cầu-cài-đặt)
4. [Hướng dẫn triển khai](#-hướng-dẫn-triển-khai)
5. [Cấu trúc dự án](#-cấu-trúc-dự-án)
6. [Chạy Pipeline](#-chạy-pipeline)
7. [Data Models](#-data-models)
8. [Troubleshooting](#-troubleshooting)

---

## 🎯 Tổng quan

SoundFlow là một data pipeline end-to-end xử lý streaming data từ ứng dụng nghe nhạc giả lập. Pipeline thu thập events từ người dùng, lưu trữ vào Data Lake (GCS), và transform thành các analytics tables trong BigQuery.

### Các loại Events

| Event Type | Mô tả | Số lượng (50K test) |
|------------|-------|---------------------|
| `listen_events` | User nghe nhạc | 24,250 |
| `page_view_events` | User xem trang | 29,335 |
| `auth_events` | Đăng nhập/đăng xuất | 435 |
| `status_change_events` | Thay đổi subscription | 21 |

---

## 🏗️ Kiến trúc hệ thống

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
| Data Generator | EventSim (Scala) | Giả lập user behavior |
| Message Queue | Redpanda (Kafka-compatible) | Buffer streaming events |
| ETL Script | Python (confluent-kafka, pyarrow) | Kafka → GCS transfer |
| Data Lake | Google Cloud Storage | Raw data storage (Parquet) |
| Data Warehouse | BigQuery | Analytics & transformations |
| Transformation | dbt | Data modeling & testing |
| Infrastructure | Terraform | Infrastructure as Code |

---

## 📦 Yêu cầu cài đặt

### Phần mềm cần thiết

| Tool | Version | Mục đích |
|------|---------|----------|
| Docker Desktop | Latest | Chạy Redpanda & EventSim |
| Python | >= 3.10 | ETL script & dbt |
| Google Cloud SDK | Latest | Tương tác với GCP |
| Terraform | >= 1.0 | Deploy infrastructure |

### Python packages

```bash
pip install -r requirements.txt
```

Packages chính:
- `confluent-kafka` - Kafka consumer
- `pyarrow` - Parquet file handling
- `pandas` - Data manipulation
- `google-cloud-storage` - GCS upload
- `dbt-bigquery` - Data transformation

### GCP Setup

1. Tạo GCP Project
2. Enable APIs:
   - Cloud Storage API
   - BigQuery API
   
3. Tạo Service Account với roles:
   - `roles/storage.objectAdmin`
   - `roles/bigquery.dataEditor`
   - `roles/bigquery.jobUser`

4. Download JSON key và lưu vào `credentials/`

---

## 🚀 Hướng dẫn triển khai

### Bước 1: Clone và cấu hình

```powershell
# Clone repository
git clone <repo-url>
cd Data_streaming_pipeline

# Tạo virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

### Bước 2: Cấu hình credentials

```powershell
# Copy service account key
mkdir credentials
# Đặt file JSON key vào credentials/dbt-sa-key.json

# Cấu hình .env
cp .env.example .env
# Chỉnh sửa các giá trị trong .env
```

### Bước 3: Deploy GCP Infrastructure (Terraform)

```powershell
cd terraform

# Initialize
terraform init

# Review plan
terraform plan

# Apply
terraform apply -auto-approve

# Bật external tables (sau khi có data)
terraform apply -var="enable_external_tables=true" -auto-approve
```

### Bước 4: Cấu hình dbt

```powershell
cd dbt

# Test connection
dbt debug --target prod

# Install packages
dbt deps
```

---

## 📁 Cấu trúc dự án

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
├── 📂 credentials/                 # Service account keys (gitignore)
│   └── dbt-sa-key.json
│
├── 📂 docs/                        # Documentation
│
├── .env                            # Environment variables
├── docker-compose.yml              # Main docker compose
└── requirements.txt                # Python dependencies
```

---

## ▶️ Chạy Pipeline

### Quick Start (Full Pipeline)

```powershell
# 1. Start Redpanda (Kafka)
docker-compose up -d redpanda

# 2. Generate events với EventSim
docker-compose up eventsim

# 3. Transfer data từ Kafka → GCS
cd spark_streaming/src
python kafka_to_gcs_python.py

# 4. Run dbt transformations
cd ../../dbt
dbt run --target prod --full-refresh

# 5. Run dbt tests
dbt test --target prod
```

### Chi tiết từng bước

#### 1️⃣ Start Redpanda

```powershell
docker-compose up -d redpanda

# Verify
docker-compose ps
# Redpanda should be healthy on port 9092
```

#### 2️⃣ Generate Events (EventSim)

Cấu hình số lượng events trong `.env`:
```env
EVENTSIM_NUSERS=500      # Số users giả lập
EVENTSIM_FROM=30         # Bắt đầu từ 30 ngày trước
EVENTSIM_TO=0            # Đến hiện tại
```

```powershell
# Chạy eventsim (không continuous - chạy 1 lần)
docker-compose up eventsim

# Hoặc chạy continuous mode
docker-compose run eventsim --continuous
```

#### 3️⃣ Upload to GCS

```powershell
cd spark_streaming/src
python kafka_to_gcs_python.py
```

Output mong đợi:
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

### Lỗi thường gặp

#### 1. Kafka connection refused
```
Error: Connection to localhost:9092 refused
```
**Giải pháp**: Kiểm tra Redpanda đang chạy
```powershell
docker-compose ps
docker-compose up -d redpanda
```

#### 2. GCS permission denied
```
Error: 403 Forbidden
```
**Giải pháp**: Kiểm tra service account có role `storage.objectAdmin`
```powershell
gcloud storage buckets add-iam-policy-binding gs://YOUR_BUCKET \
  --member="serviceAccount:YOUR_SA@PROJECT.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

#### 3. BigQuery schema mismatch
```
Error: Parquet column 'X' has type BYTE_ARRAY which does not match INT64
```
**Giải pháp**: Xóa và tạo lại external table
```powershell
bq rm -f -t PROJECT:raw.ext_listen_events
# Tạo lại với autodetect
```

#### 4. dbt model not found
```
Error: Relation does not exist
```
**Giải pháp**: Chạy với `--full-refresh`
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
