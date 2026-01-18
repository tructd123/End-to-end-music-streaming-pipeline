# 🎵 SoundFlow Pipeline - Hướng Dẫn Chi Tiết

## Mục Lục
1. [Tổng Quan](#1-tổng-quan)
2. [Yêu Cầu Hệ Thống](#2-yêu-cầu-hệ-thống)
3. [Cài Đặt Môi Trường](#3-cài-đặt-môi-trường)
4. [Deploy Infrastructure với Terraform](#4-deploy-infrastructure-với-terraform)
5. [Chạy Data Generator (EventSim)](#5-chạy-data-generator-eventsim)
6. [Chạy Spark Streaming](#6-chạy-spark-streaming)
7. [Chạy dbt Transformations](#7-chạy-dbt-transformations)
8. [Monitoring & Troubleshooting](#8-monitoring--troubleshooting)
9. [Dọn Dẹp Resources](#9-dọn-dẹp-resources)

---

## 1. Tổng Quan

### Kiến Trúc Pipeline

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  EventSim   │───▶│  Pub/Sub    │───▶│  Dataproc   │───▶│    GCS      │───▶│  BigQuery   │
│  (Generator)│    │  (Kafka)    │    │  (Spark)    │    │ (Data Lake) │    │    (DWH)    │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                                                   │
                                                                                   ▼
                                                                            ┌─────────────┐
                                                                            │     dbt     │
                                                                            │(Transforms) │
                                                                            └─────────────┘
```

### Các Loại Events

| Event Type | Mô Tả | Pub/Sub Topic |
|------------|-------|---------------|
| `listen_events` | User nghe nhạc | `listen-events` |
| `page_view_events` | User xem trang | `page-view-events` |
| `auth_events` | Đăng nhập/đăng xuất | `auth-events` |
| `status_change_events` | Thay đổi subscription | `status-change-events` |

---

## 2. Yêu Cầu Hệ Thống

### 2.1 Phần Mềm Cần Cài Đặt

| Tool | Version | Mục Đích |
|------|---------|----------|
| **Google Cloud SDK** | Latest | Tương tác với GCP |
| **Terraform** | >= 1.0 | Infrastructure as Code |
| **Docker** | Latest | Chạy EventSim |
| **Python** | >= 3.9 | Chạy dbt |
| **dbt-bigquery** | >= 1.5 | Data transformations |

### 2.2 Cài Đặt Tools

```powershell
# 1. Cài đặt Google Cloud SDK
# Download từ: https://cloud.google.com/sdk/docs/install

# 2. Cài đặt Terraform
choco install terraform
# hoặc download từ: https://terraform.io/downloads

# 3. Cài đặt Docker Desktop
# Download từ: https://www.docker.com/products/docker-desktop

# 4. Cài đặt Python dependencies
pip install dbt-bigquery google-cloud-pubsub google-cloud-storage
```

### 2.3 GCP Project Setup

```powershell
# Đăng nhập GCP
gcloud auth login
gcloud auth application-default login

# Set project
gcloud config set project YOUR_PROJECT_ID

# Enable required APIs
gcloud services enable \
    compute.googleapis.com \
    storage.googleapis.com \
    bigquery.googleapis.com \
    pubsub.googleapis.com \
    dataproc.googleapis.com \
    artifactregistry.googleapis.com
```

---

## 3. Cài Đặt Môi Trường

### 3.1 Clone Repository

```powershell
git clone https://github.com/your-repo/Data_streaming_pipeline.git
cd Data_streaming_pipeline
```

### 3.2 Cấu Hình Terraform Variables

```powershell
cd terraform

# Copy file example
cp terraform.tfvars.example terraform.tfvars

# Chỉnh sửa terraform.tfvars
notepad terraform.tfvars
```

**Nội dung terraform.tfvars:**
```hcl
# GCP Configuration
project_id      = "your-gcp-project-id"
region          = "asia-southeast1"
bq_location     = "asia-southeast1"
environment     = "dev"

# GCS Bucket (phải unique globally)
gcs_bucket_name = "your-unique-bucket-name"

# Dataproc Configuration
enable_dataproc                   = true
dataproc_master_machine_type      = "n1-standard-4"
dataproc_worker_machine_type      = "n1-standard-4"
dataproc_num_workers              = 2
dataproc_num_preemptible_workers  = 0

# Các features khác
enable_external_tables    = false  # Bật sau khi có data
enable_dataproc_autoscaling = false
```

---

## 4. Deploy Infrastructure với Terraform

### 4.1 Initialize Terraform

```powershell
cd E:\Individual\Data_streaming_pipeline\terraform

# Initialize
terraform init

# Xem plan
terraform plan
```

### 4.2 Apply Infrastructure

```powershell
# Deploy tất cả resources
terraform apply

# Hoặc auto-approve
terraform apply -auto-approve
```

### 4.3 Kiểm Tra Resources Đã Tạo

```powershell
# Xem outputs
terraform output

# Kiểm tra Dataproc cluster
gcloud dataproc clusters list --region=asia-southeast1

# Kiểm tra Pub/Sub topics
gcloud pubsub topics list

# Kiểm tra GCS bucket
gsutil ls gs://YOUR_BUCKET_NAME/
```

### 4.4 Expected Output

```
bigquery_datasets = {
  "intermediate" = "intermediate"
  "marts" = "marts"
  "raw" = "raw"
  "staging" = "staging"
}
dataproc_cluster = {
  "name" = "soundflow-spark-dev"
  "num_workers" = 2
  ...
}
pubsub_topics = {
  "auth_events" = "auth-events"
  "listen_events" = "listen-events"
  "page_view_events" = "page-view-events"
  "status_change_events" = "status-change-events"
}
```

---

## 5. Chạy Data Generator (EventSim)

### 5.1 Option A: Chạy Local với Docker

```powershell
cd E:\Individual\Data_streaming_pipeline\eventsim

# Build Docker image
docker build -t eventsim -f docker/Dockerfile .

# Chạy EventSim gửi events đến Pub/Sub
docker run -it --rm \
    -v ${PWD}/examples:/opt/eventsim/config \
    -e GOOGLE_APPLICATION_CREDENTIALS=/opt/eventsim/keys/sa-key.json \
    -v E:\Individual\Data_streaming_pipeline\terraform\keys:/opt/eventsim/keys \
    eventsim \
    --config /opt/eventsim/config/example-config.json \
    --kafkaBrokerList projects/YOUR_PROJECT_ID/topics/listen-events \
    --continuous
```

### 5.2 Option B: Publish Test Events Manually

```powershell
# Test publish một message
gcloud pubsub topics publish listen-events \
    --project=YOUR_PROJECT_ID \
    --message='{
        "ts": 1705500000000,
        "userId": "user123",
        "sessionId": "session456",
        "song": "Test Song",
        "artist": "Test Artist",
        "duration": 180.5
    }'

# Publish nhiều test messages
$events = @(
    '{"ts":1705500000000,"userId":"user1","sessionId":"s1","song":"Song1","artist":"Artist1","duration":200}',
    '{"ts":1705500001000,"userId":"user2","sessionId":"s2","song":"Song2","artist":"Artist2","duration":180}',
    '{"ts":1705500002000,"userId":"user3","sessionId":"s3","song":"Song3","artist":"Artist3","duration":240}'
)

foreach ($event in $events) {
    gcloud pubsub topics publish listen-events --project=YOUR_PROJECT_ID --message=$event
    Start-Sleep -Milliseconds 100
}
```

### 5.3 Verify Messages in Pub/Sub

```powershell
# Xem messages trong subscription (không ack)
gcloud pubsub subscriptions pull listen-events-spark-sub \
    --project=YOUR_PROJECT_ID \
    --limit=10 \
    --auto-ack=false
```

---

## 6. Chạy Spark Streaming

### 6.1 Submit Spark Job lên Dataproc

```powershell
# Lấy command từ terraform output
terraform output dataproc_submit_command

# Submit job
gcloud dataproc jobs submit pyspark gs://YOUR_BUCKET_NAME/spark-apps/streaming_to_gcs_pubsub.py `
    --project=YOUR_PROJECT_ID `
    --region=asia-southeast1 `
    --cluster=soundflow-spark-dev `
    --properties="spark.jars.packages=com.google.cloud.spark:spark-pubsub_2.12:0.21.0"
```

### 6.2 Submit Job với Tất Cả Event Types

```powershell
# Listen Events
gcloud dataproc jobs submit pyspark gs://YOUR_BUCKET/spark-apps/streaming_to_gcs_pubsub.py `
    --project=YOUR_PROJECT_ID --region=asia-southeast1 --cluster=soundflow-spark-dev `
    --properties="spark.jars.packages=com.google.cloud.spark:spark-pubsub_2.12:0.21.0" `
    -- --event-type=listen_events

# Page View Events
gcloud dataproc jobs submit pyspark gs://YOUR_BUCKET/spark-apps/streaming_to_gcs_pubsub.py `
    --project=YOUR_PROJECT_ID --region=asia-southeast1 --cluster=soundflow-spark-dev `
    --properties="spark.jars.packages=com.google.cloud.spark:spark-pubsub_2.12:0.21.0" `
    -- --event-type=page_view_events

# Auth Events
gcloud dataproc jobs submit pyspark gs://YOUR_BUCKET/spark-apps/streaming_to_gcs_pubsub.py `
    --project=YOUR_PROJECT_ID --region=asia-southeast1 --cluster=soundflow-spark-dev `
    --properties="spark.jars.packages=com.google.cloud.spark:spark-pubsub_2.12:0.21.0" `
    -- --event-type=auth_events
```

### 6.3 Monitor Spark Jobs

```powershell
# List running jobs
gcloud dataproc jobs list --region=asia-southeast1 --state-filter=ACTIVE

# Xem logs của job
gcloud dataproc jobs describe JOB_ID --region=asia-southeast1

# Xem Spark UI
# Mở URL từ output: dataproc_cluster.web_ui
```

### 6.4 Verify Data trong GCS

```powershell
# Kiểm tra files đã được ghi
gsutil ls -r gs://YOUR_BUCKET/raw/

# Xem cấu trúc thư mục
gsutil ls gs://YOUR_BUCKET/raw/listen_events/
gsutil ls gs://YOUR_BUCKET/raw/page_view_events/
gsutil ls gs://YOUR_BUCKET/raw/auth_events/
gsutil ls gs://YOUR_BUCKET/raw/status_change_events/

# Xem nội dung file parquet
gsutil cat gs://YOUR_BUCKET/raw/listen_events/year=2024/month=01/day=17/*.parquet | head
```

---

## 7. Chạy dbt Transformations

### 7.1 Enable BigQuery External Tables

Sau khi có data trong GCS, enable external tables:

```powershell
cd E:\Individual\Data_streaming_pipeline\terraform

# Bật external tables
terraform apply -var="enable_external_tables=true"
```

### 7.2 Cấu Hình dbt Profile

```powershell
cd E:\Individual\Data_streaming_pipeline\dbt

# Tạo profiles.yml
mkdir -p ~/.dbt
```

**~/.dbt/profiles.yml:**
```yaml
soundflow:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: YOUR_PROJECT_ID
      dataset: staging
      location: asia-southeast1
      keyfile: E:/Individual/Data_streaming_pipeline/terraform/keys/dbt-sa-key.json
      threads: 4
      timeout_seconds: 300
    
    prod:
      type: bigquery
      method: service-account
      project: YOUR_PROJECT_ID
      dataset: staging
      location: asia-southeast1
      keyfile: E:/Individual/Data_streaming_pipeline/terraform/keys/dbt-sa-key.json
      threads: 8
      timeout_seconds: 300
```

### 7.3 Chạy dbt

```powershell
cd E:\Individual\Data_streaming_pipeline\dbt

# Install dependencies
dbt deps

# Test connection
dbt debug

# Chạy staging models
dbt run --select staging.*

# Chạy intermediate models
dbt run --select intermediate.*

# Chạy marts models
dbt run --select marts.*

# Hoặc chạy tất cả
dbt run

# Chạy tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### 7.4 Verify Data trong BigQuery

```powershell
# Query raw data
bq query --use_legacy_sql=false '
SELECT * FROM `YOUR_PROJECT_ID.raw.listen_events` LIMIT 10
'

# Query staging data
bq query --use_legacy_sql=false '
SELECT * FROM `YOUR_PROJECT_ID.staging.stg_listen_events` LIMIT 10
'

# Query marts data
bq query --use_legacy_sql=false '
SELECT * FROM `YOUR_PROJECT_ID.marts.dim_users` LIMIT 10
'
```

---

## 8. Monitoring & Troubleshooting

### 8.1 GCP Console Links

| Resource | URL |
|----------|-----|
| **Dataproc** | https://console.cloud.google.com/dataproc/clusters?project=YOUR_PROJECT |
| **Pub/Sub** | https://console.cloud.google.com/cloudpubsub/topic/list?project=YOUR_PROJECT |
| **BigQuery** | https://console.cloud.google.com/bigquery?project=YOUR_PROJECT |
| **GCS** | https://console.cloud.google.com/storage/browser?project=YOUR_PROJECT |
| **Logging** | https://console.cloud.google.com/logs?project=YOUR_PROJECT |

### 8.2 Common Issues & Solutions

#### Issue 1: Dataproc Cluster Creation Failed
```
Error: Zone does not have enough resources
```
**Solution:** Đổi zone trong `dataproc.tf`:
```hcl
zone = "${var.region}-b"  # Thử zone khác: -a, -b, -c
```

#### Issue 2: Pub/Sub Permission Denied
```
Error: Permission denied on topic
```
**Solution:** Kiểm tra IAM roles:
```powershell
gcloud projects get-iam-policy YOUR_PROJECT_ID \
    --flatten="bindings[].members" \
    --filter="bindings.members:soundflow-spark@"
```

#### Issue 3: BigQuery External Table Error
```
Error: No files matched pattern
```
**Solution:** Đảm bảo có data trong GCS trước khi enable external tables:
```powershell
gsutil ls gs://YOUR_BUCKET/raw/listen_events/
```

#### Issue 4: Spark Job Failed
```
Error: Class not found: pubsub connector
```
**Solution:** Thêm packages property:
```powershell
--properties="spark.jars.packages=com.google.cloud.spark:spark-pubsub_2.12:0.21.0"
```

### 8.3 Useful Commands

```powershell
# Xem Dataproc cluster logs
gcloud logging read "resource.type=cloud_dataproc_cluster" --limit=50

# Xem Pub/Sub metrics
gcloud pubsub topics list-subscriptions listen-events

# Check unacked messages
gcloud pubsub subscriptions pull listen-events-spark-sub --auto-ack=false --limit=5

# Monitor GCS writes
gsutil -m ls -l gs://YOUR_BUCKET/raw/**/*.parquet | tail -20
```

---

## 9. Dọn Dẹp Resources

### 9.1 Stop Spark Jobs

```powershell
# List active jobs
gcloud dataproc jobs list --region=asia-southeast1 --state-filter=ACTIVE

# Cancel job
gcloud dataproc jobs kill JOB_ID --region=asia-southeast1
```

### 9.2 Delete Dataproc Cluster (Tiết Kiệm Chi Phí)

```powershell
# Tạm thời disable Dataproc trong terraform
terraform apply -var="enable_dataproc=false"

# Hoặc delete trực tiếp
gcloud dataproc clusters delete soundflow-spark-dev --region=asia-southeast1
```

### 9.3 Destroy All Infrastructure

```powershell
cd E:\Individual\Data_streaming_pipeline\terraform

# Xem sẽ xóa những gì
terraform plan -destroy

# Xóa tất cả resources
terraform destroy

# Xác nhận: yes
```

### 9.4 Xóa Data trong GCS (Optional)

```powershell
# Xóa tất cả data trong bucket
gsutil -m rm -r gs://YOUR_BUCKET/**

# Xóa bucket
gsutil rb gs://YOUR_BUCKET
```

---

## 📋 Quick Reference

### Terraform Commands
```powershell
terraform init              # Initialize
terraform plan              # Preview changes
terraform apply             # Deploy
terraform destroy           # Remove all
terraform output            # Show outputs
```

### gcloud Commands
```powershell
gcloud dataproc clusters list --region=asia-southeast1
gcloud dataproc jobs list --region=asia-southeast1
gcloud pubsub topics list
gcloud pubsub subscriptions list
gsutil ls gs://BUCKET/
```

### dbt Commands
```powershell
dbt debug          # Test connection
dbt deps           # Install dependencies
dbt run            # Run all models
dbt test           # Run tests
dbt docs generate  # Generate docs
dbt docs serve     # Serve docs locally
```

---

## 📞 Support

- **GCP Documentation**: https://cloud.google.com/docs
- **Terraform GCP Provider**: https://registry.terraform.io/providers/hashicorp/google
- **dbt Documentation**: https://docs.getdbt.com
- **Spark Documentation**: https://spark.apache.org/docs/latest

---

## 📝 Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2024-01-17 | Initial release |
| 1.1.0 | 2024-01-17 | Added Dataproc configuration |
| 1.2.0 | 2024-01-17 | Fixed init script for Unix line endings |
