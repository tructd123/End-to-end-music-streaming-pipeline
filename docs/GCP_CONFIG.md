# ⚙️ GCP Configuration Guide

## 📋 Project Information

| Property | Value |
|----------|-------|
| **Project ID** | `graphic-boulder-483814-g7` |
| **Region** | `asia-southeast1` |
| **GCS Bucket** | `tf-state-soundflow-123` |

---

## 🔐 Service Accounts

### dbt Service Account
- **Email**: `soundflow-dbt@graphic-boulder-483814-g7.iam.gserviceaccount.com`
- **Key file**: `credentials/dbt-sa-key.json`
- **Roles**:
  - `roles/bigquery.dataEditor`
  - `roles/bigquery.jobUser`
  - `roles/storage.objectAdmin`

---

## 📦 GCS Bucket Structure

```
gs://tf-state-soundflow-123/
├── raw/
│   ├── listen_events/
│   │   └── year=YYYY/month=MM/day=DD/hour=HH/data.parquet
│   ├── page_view_events/
│   │   └── year=YYYY/month=MM/day=DD/hour=HH/data.parquet
│   ├── auth_events/
│   │   └── year=YYYY/month=MM/day=DD/hour=HH/data.parquet
│   └── status_change_events/
│       └── year=YYYY/month=MM/day=DD/hour=HH/data.parquet
└── terraform/
    └── state files
```

### Partition Format
- **Hive-style partitioning**: `year=YYYY/month=MM/day=DD/hour=HH`
- **File format**: Parquet with millisecond timestamps

---

## 🗄️ BigQuery Datasets

| Dataset | Schema | Purpose |
|---------|--------|---------|
| `raw` | External tables | Raw data from GCS |
| `staging_staging` | Views | Cleaned & standardized |
| `staging_intermediate` | Views | Aggregated metrics |
| `staging_marts` | Tables | Analytics-ready |

### External Tables (raw dataset)

| Table | Source URI |
|-------|------------|
| `ext_listen_events` | `gs://bucket/raw/listen_events/*` |
| `ext_page_view_events` | `gs://bucket/raw/page_view_events/*` |
| `ext_auth_events` | `gs://bucket/raw/auth_events/*` |
| `ext_status_change_events` | `gs://bucket/raw/status_change_events/*` |

---

## 🔧 Terraform Resources

### Enabled Resources
- ✅ GCS Bucket
- ✅ BigQuery Datasets (raw, staging, intermediate, marts)
- ✅ Service Accounts
- ✅ IAM Bindings

### Conditional Resources
- ⚡ External Tables (`enable_external_tables = true`)
- ❌ Dataproc Cluster (not used - using Python script instead)

### Deploy Commands

```powershell
cd terraform

# Initialize
terraform init

# Basic deployment
terraform apply -auto-approve

# Enable external tables
terraform apply -var="enable_external_tables=true" -auto-approve

# Destroy all resources
terraform destroy -auto-approve
```

---

## 💰 Estimated Costs

| Resource | Cost | Notes |
|----------|------|-------|
| **GCS Storage** | ~$0.02/GB/month | Free tier: 5GB |
| **BigQuery Storage** | ~$0.02/GB/month | Free tier: 10GB |
| **BigQuery Query** | ~$5/TB scanned | Free tier: 1TB/month |

> 💡 **With 50K events (~5MB data)**: Entirely within free tier

---

## 🔗 Console Links

- [BigQuery Console](https://console.cloud.google.com/bigquery?project=graphic-boulder-483814-g7)
- [Cloud Storage Browser](https://console.cloud.google.com/storage/browser/tf-state-soundflow-123)
- [IAM & Admin](https://console.cloud.google.com/iam-admin/iam?project=graphic-boulder-483814-g7)
- [Billing](https://console.cloud.google.com/billing)

---

## 🛠️ Useful gcloud Commands

### Authentication
```powershell
# Login
gcloud auth login

# Set project
gcloud config set project graphic-boulder-483814-g7

# Application default credentials (for local development)
gcloud auth application-default login
```

### Storage
```powershell
# List bucket contents
gsutil ls -r gs://tf-state-soundflow-123/raw/

# Delete all data in a path
gsutil -m rm -r gs://tf-state-soundflow-123/raw/listen_events/

# Copy file to GCS
gsutil cp local_file.parquet gs://tf-state-soundflow-123/raw/
```

### BigQuery
```powershell
# List datasets
bq ls --project_id=graphic-boulder-483814-g7

# Show table schema
bq show --format=prettyjson graphic-boulder-483814-g7:raw.ext_listen_events

# Query
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM raw.ext_listen_events"

# Delete table
bq rm -f -t graphic-boulder-483814-g7:raw.ext_listen_events

# Create external table
bq mk --external_table_definition=def.json PROJECT:DATASET.TABLE
```

---

## 🔄 Recreate External Table

When you need to reset the external table schema:

```powershell
# 1. Delete existing table
bq rm -f -t graphic-boulder-483814-g7:raw.ext_listen_events

# 2. Create definition file
python -c "
import json
with open('ext_def.json', 'w') as f:
    json.dump({
        'autodetect': True,
        'sourceFormat': 'PARQUET',
        'sourceUris': ['gs://tf-state-soundflow-123/raw/listen_events/*'],
        'hivePartitioningOptions': {
            'mode': 'AUTO',
            'sourceUriPrefix': 'gs://tf-state-soundflow-123/raw/listen_events'
        }
    }, f)
"

# 3. Create table
bq mk --external_table_definition=ext_def.json graphic-boulder-483814-g7:raw.ext_listen_events

# 4. Cleanup
Remove-Item ext_def.json
```

---

## 📊 Environment Variables

File `.env`:
```env
# GCP Configuration
GCP_PROJECT_ID=graphic-boulder-483814-g7
GCP_REGION=asia-southeast1
GCS_BUCKET=tf-state-soundflow-123
GOOGLE_APPLICATION_CREDENTIALS=E:\Individual\Data_streaming_pipeline\credentials\dbt-sa-key.json

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# EventSim Configuration
EVENTSIM_NUSERS=500
EVENTSIM_FROM=30
EVENTSIM_TO=0
```
