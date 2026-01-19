# 🚀 Quick Start Guide

Hướng dẫn nhanh để chạy pipeline từ đầu đến cuối.

## Prerequisites

- Docker Desktop đang chạy
- Python 3.10+ với virtual environment
- GCP credentials đã cấu hình

## 5 Bước Chạy Pipeline

### 1️⃣ Activate Environment

```powershell
cd E:\Individual\Data_streaming_pipeline
.venv\Scripts\Activate.ps1
```

### 2️⃣ Start Kafka (Redpanda)

```powershell
docker-compose up -d redpanda

# Verify
docker-compose ps
```

### 3️⃣ Generate Events

```powershell
# Chạy eventsim để tạo ~50K events
docker-compose up eventsim

# Đợi cho đến khi hoàn thành
# Output: "End Time: ..."
```

### 4️⃣ Upload to GCS

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

### 5️⃣ Run dbt

```powershell
cd ../../dbt

# Transform data
dbt run --target prod --full-refresh

# Run tests
dbt test --target prod
```

Expected output:
```
Done. PASS=13 WARN=0 ERROR=0 SKIP=0 TOTAL=13
Done. PASS=31 WARN=0 ERROR=0 SKIP=0 TOTAL=31
```

---

## 🎉 Done!

Data đã sẵn sàng trong BigQuery:
- **13 models** đã tạo
- **31 tests** passed

### View Results

```powershell
# Query top songs
bq query --use_legacy_sql=false "
SELECT song, artist, total_plays 
FROM staging_marts.mart_top_songs 
LIMIT 10
"
```

---

## 🧹 Cleanup

```powershell
# Stop containers
docker-compose down

# (Optional) Delete GCS data
gsutil -m rm -r gs://tf-state-soundflow-123/raw/
```

---

## 📝 Cấu hình số lượng Events

Chỉnh sửa `.env`:

```env
# Ít events (test nhanh)
EVENTSIM_NUSERS=50
EVENTSIM_FROM=1
EVENTSIM_TO=0

# Nhiều events (production-like)
EVENTSIM_NUSERS=1000
EVENTSIM_FROM=90
EVENTSIM_TO=0
```

Sau khi chỉnh, rebuild eventsim:
```powershell
docker-compose up --build eventsim
```
