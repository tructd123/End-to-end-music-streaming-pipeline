# SoundFlow - GCP Pipeline Operations Guide

## 📊 Tổng quan Hệ thống

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SoundFlow Data Pipeline                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌───────────┐ │
│  │   EventSim   │───▶│   Pub/Sub    │───▶│   Dataproc   │───▶│    GCS    │ │
│  │  (Generator) │    │   (Queue)    │    │   (Spark)    │    │  (Lake)   │ │
│  └──────────────┘    └──────────────┘    └──────────────┘    └─────┬─────┘ │
│                                                                     │       │
│                                                                     ▼       │
│                      ┌──────────────┐    ┌──────────────┐    ┌───────────┐ │
│                      │   Dagster    │◀──▶│     dbt      │───▶│  BigQuery │ │
│                      │ (Orchestrate)│    │ (Transform)  │    │  (Marts)  │ │
│                      └──────────────┘    └──────────────┘    └───────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 💰 Chi phí GCP Resources

| Resource | Chi phí/giờ | Chi phí/ngày | Trạng thái hiện tại |
|----------|------------|--------------|---------------------|
| **Dataproc Cluster** | ~$0.50/hr | ~$12/day | ⏹️ TẮT |
| GCS Storage | ~$0.02/GB/tháng | - | ✅ BẬT (miễn phí tier) |
| Pub/Sub | ~$40/TB | - | ✅ BẬT (miễn phí tier) |
| BigQuery | ~$5/TB query | - | ✅ BẬT (miễn phí tier) |

> ⚠️ **Dataproc là thành phần tốn chi phí nhất!** Chỉ bật khi cần xử lý data.

## 🔧 Thông tin Cấu hình

```yaml
# GCP Project
project_id: graphic-boulder-483814-g7
region: asia-southeast1
zone: asia-southeast1-b

# GCS Bucket
bucket: tf-state-soundflow-123
paths:
  raw: gs://tf-state-soundflow-123/raw/
  processed: gs://tf-state-soundflow-123/processed/
  checkpoints: gs://tf-state-soundflow-123/checkpoints/

# Pub/Sub Topics
topics:
  - listen-events
  - page-view-events  
  - auth-events
  - status-change-events

# BigQuery Datasets
datasets:
  - raw          # Raw data từ GCS
  - staging      # dbt staging models
  - intermediate # dbt intermediate
  - marts        # Final analytics tables

# Service Accounts
service_accounts:
  spark: soundflow-spark@graphic-boulder-483814-g7.iam.gserviceaccount.com
  dbt: soundflow-dbt@graphic-boulder-483814-g7.iam.gserviceaccount.com
  dagster: soundflow-dagster@graphic-boulder-483814-g7.iam.gserviceaccount.com
```

---

## 🚀 Cách Chạy Pipeline

### Bước 1: Bật Dataproc Cluster (khi cần xử lý data)

```powershell
cd E:\Individual\Data_streaming_pipeline\terraform

# Bật cluster
terraform apply -var="enable_dataproc=true" -auto-approve

# Đợi ~5 phút để cluster ready
```

### Bước 2: Publish Test Events vào Pub/Sub

```powershell
cd E:\Individual\Data_streaming_pipeline

# Chạy script publish events
.venv/Scripts/python.exe publish_events.py
```

### Bước 3: Submit Spark Job để xử lý data

```powershell
# Upload spark script lên GCS
gsutil cp spark_streaming/src/streaming_to_gcs_pubsub.py gs://tf-state-soundflow-123/spark-apps/

# Submit job
gcloud dataproc jobs submit pyspark `
  gs://tf-state-soundflow-123/spark-apps/test_simple.py `
  --project=graphic-boulder-483814-g7 `
  --region=asia-southeast1 `
  --cluster=soundflow-spark-dev
```

### Bước 4: Kiểm tra kết quả

```powershell
# Xem files trong GCS
gsutil ls gs://tf-state-soundflow-123/test/

# Xem nội dung file
gsutil cat gs://tf-state-soundflow-123/test/pubsub_test_*.json
```

### Bước 5: TẮT Dataproc để tiết kiệm chi phí

```powershell
cd E:\Individual\Data_streaming_pipeline\terraform

# Tắt cluster
terraform apply -var="enable_dataproc=false" -auto-approve
```

---

## ⏹️ Dừng Pipeline Hoàn Toàn

### Option 1: Chỉ tắt Dataproc (giữ data)
```powershell
cd E:\Individual\Data_streaming_pipeline\terraform
terraform apply -var="enable_dataproc=false" -auto-approve
```
- ✅ Giữ nguyên: GCS, Pub/Sub, BigQuery, Service Accounts
- ⏹️ Tắt: Dataproc cluster (tiết kiệm ~$12/ngày)

### Option 2: Xóa toàn bộ infrastructure
```powershell
cd E:\Individual\Data_streaming_pipeline\terraform
terraform destroy -auto-approve
```
- ⚠️ **CẢNH BÁO**: Xóa hết data trong GCS và BigQuery!

---

## 📁 Cấu trúc Files quan trọng

```
Data_streaming_pipeline/
├── terraform/                    # Infrastructure as Code
│   ├── main.tf                  # Provider config
│   ├── variables.tf             # Variables (enable_dataproc)
│   ├── gcs.tf                   # GCS bucket
│   ├── pubsub.tf                # Pub/Sub topics
│   ├── bigquery.tf              # BigQuery datasets
│   ├── iam.tf                   # Service accounts & permissions
│   └── dataproc.tf              # Dataproc cluster (conditional)
│
├── spark_streaming/src/
│   ├── streaming_to_gcs_pubsub.py  # Main streaming job
│   └── test_simple.py              # Test script
│
├── credentials/                  # Service account keys (gitignore)
│   ├── spark-sa-key.json
│   ├── dbt-sa-key.json
│   └── dagster-sa-key.json
│
├── publish_events.py            # Script publish test events
└── docs/
    ├── GCP_OPERATIONS.md        # This file
    └── GCP_ARCHITECTURE.md      # Architecture docs
```

---

## 🔍 Commands Hữu ích

### Kiểm tra trạng thái
```powershell
# Xem Dataproc clusters
gcloud dataproc clusters list --region=asia-southeast1 --project=graphic-boulder-483814-g7

# Xem Pub/Sub topics
gcloud pubsub topics list --project=graphic-boulder-483814-g7

# Xem BigQuery datasets
bq ls --project_id=graphic-boulder-483814-g7

# Xem GCS bucket
gsutil ls gs://tf-state-soundflow-123/
```

### Pull messages từ Pub/Sub (manual)
```powershell
gcloud pubsub subscriptions pull listen-events-spark-sub `
  --project=graphic-boulder-483814-g7 `
  --limit=10 `
  --auto-ack
```

### Xem Dataproc job logs
```powershell
gcloud dataproc jobs list `
  --project=graphic-boulder-483814-g7 `
  --region=asia-southeast1
```

---

## ✅ Checklist Trước khi Tắt máy

- [ ] Tắt Dataproc cluster: `terraform apply -var="enable_dataproc=false" -auto-approve`
- [ ] Verify cluster đã tắt: `gcloud dataproc clusters list --region=asia-southeast1`
- [ ] (Optional) Xem billing: https://console.cloud.google.com/billing

---

## 🔗 Links Console

- [Dataproc](https://console.cloud.google.com/dataproc/clusters?project=graphic-boulder-483814-g7)
- [GCS](https://console.cloud.google.com/storage/browser/tf-state-soundflow-123)
- [Pub/Sub](https://console.cloud.google.com/cloudpubsub/topic/list?project=graphic-boulder-483814-g7)
- [BigQuery](https://console.cloud.google.com/bigquery?project=graphic-boulder-483814-g7)
- [Billing](https://console.cloud.google.com/billing)
