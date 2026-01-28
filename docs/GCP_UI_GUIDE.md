# 🖥️ GCP Console UI Guide

A comprehensive guide to view, query, and manage your data through the GCP Console UI after successful pipeline testing.

---

## 📋 Quick Links

| Service | Direct Link |
|---------|-------------|
| **BigQuery** | [console.cloud.google.com/bigquery](https://console.cloud.google.com/bigquery?project=graphic-boulder-483814-g7) |
| **Cloud Storage** | [console.cloud.google.com/storage](https://console.cloud.google.com/storage/browser/tf-state-soundflow-123) |
| **IAM & Admin** | [console.cloud.google.com/iam-admin](https://console.cloud.google.com/iam-admin/iam?project=graphic-boulder-483814-g7) |
| **Billing** | [console.cloud.google.com/billing](https://console.cloud.google.com/billing) |

---

## 🗄️ BigQuery Console

### Access BigQuery

1. Go to [BigQuery Console](https://console.cloud.google.com/bigquery?project=graphic-boulder-483814-g7)
2. In the left panel, expand your project: `graphic-boulder-483814-g7`

### View Datasets

You should see these datasets:

```
📁 graphic-boulder-483814-g7
├── 📂 raw                      # External tables (GCS data)
├── 📂 staging_staging          # Staging views
├── 📂 staging_intermediate     # Intermediate views  
└── 📂 staging_marts            # Analytics tables
```

### View Tables & Schema

1. Click on a dataset (e.g., `raw`)
2. Click on a table (e.g., `ext_listen_events`)
3. View tabs:
   - **Schema** - Column names, types
   - **Details** - Row count, size, partitioning info
   - **Preview** - Sample data (first 50 rows)

### Run SQL Queries

1. Click **"+ Compose new query"** (top right)
2. Enter your SQL query
3. Click **"Run"** or press `Ctrl+Enter`

#### Sample Queries

**Count records in each table:**
```sql
-- Raw data count
SELECT 'listen_events' as table_name, COUNT(*) as cnt FROM `raw.ext_listen_events`
UNION ALL
SELECT 'page_view_events', COUNT(*) FROM `raw.ext_page_view_events`
UNION ALL
SELECT 'auth_events', COUNT(*) FROM `raw.ext_auth_events`
UNION ALL
SELECT 'status_change_events', COUNT(*) FROM `raw.ext_status_change_events`;
```

**Top 10 most played songs:**
```sql
SELECT song, artist, total_plays, unique_listeners
FROM `staging_marts.mart_top_songs`
ORDER BY total_plays DESC
LIMIT 10;
```

**User activity by location:**
```sql
SELECT city, state, total_listens, unique_users
FROM `staging_marts.mart_location_analytics`
ORDER BY total_listens DESC
LIMIT 20;
```

**Hourly metrics:**
```sql
SELECT *
FROM `staging_marts.mart_hourly_metrics`
ORDER BY event_hour DESC
LIMIT 24;
```

**Active users with engagement tier:**
```sql
SELECT user_id, first_name, last_name, total_listens, engagement_tier
FROM `staging_marts.mart_active_users`
ORDER BY total_listens DESC
LIMIT 20;
```

### Query Results Actions

After running a query:
- **Save results** → Click "Save Results" → Choose CSV, JSON, or BigQuery table
- **Download** → Click "Download" → Choose format
- **Explore in Sheets** → Opens in Google Sheets
- **Save query** → Click "Save" → Save for later use

### View Query History

1. Click **"Personal history"** in left panel
2. See all your past queries
3. Click any query to re-run it

---

## 📦 Cloud Storage Console

### Access GCS

1. Go to [Cloud Storage](https://console.cloud.google.com/storage/browser/tf-state-soundflow-123)
2. You'll see the bucket: `tf-state-soundflow-123`

### Browse Data Structure

```
📁 tf-state-soundflow-123/
├── 📂 raw/
│   ├── 📂 listen_events/
│   │   └── 📂 year=2025/
│   │       └── 📂 month=12/
│   │           └── 📂 day=XX/
│   │               └── 📂 hour=XX/
│   │                   └── 📄 data.parquet
│   ├── 📂 page_view_events/
│   ├── 📂 auth_events/
│   └── 📂 status_change_events/
└── 📂 terraform/
```

### View Parquet Files

1. Navigate to a folder (e.g., `raw/listen_events/year=2025/month=12/day=29/hour=00/`)
2. Click on `data.parquet`
3. View file details:
   - **Size**
   - **Created time**
   - **Storage class**
   - **Public access** (should be "Not public")

### Download Files

1. Click on the file
2. Click **"Download"** button
3. Or copy `gsutil` command from the UI

### Check Storage Usage

1. Go to bucket root
2. Click **"..."** (three dots) → **"View size"**
3. See total storage used

### Delete Data (Cleanup)

1. Select folder(s) to delete
2. Click **"Delete"** button
3. Confirm deletion

⚠️ **Warning**: Deleting GCS data will make BigQuery external tables empty!

---

## 📊 Monitoring & Logs

### BigQuery Job History

1. In BigQuery, click **"Job history"** (bottom panel)
2. See all running/completed jobs
3. Click on a job to see:
   - Query text
   - Bytes processed
   - Duration
   - Errors (if any)

### Cloud Logging

1. Go to [Logging](https://console.cloud.google.com/logs?project=graphic-boulder-483814-g7)
2. Filter by resource:
   - `BigQuery` - Query logs
   - `Cloud Storage` - Access logs

### Billing & Cost

1. Go to [Billing Reports](https://console.cloud.google.com/billing)
2. Select your billing account
3. Click **"Reports"**
4. Filter by:
   - Service: BigQuery, Cloud Storage
   - Time range: Last 7 days

---

## 🔧 Common Operations

### Refresh External Table Schema

If Parquet schema changed:

1. Go to BigQuery
2. Find the external table
3. Click **"..."** → **"Delete table"**
4. Recreate using Terraform or `bq` command

### Check Table Schema

1. Click on table
2. Go to **"Schema"** tab
3. Verify column names and types

### Export Query Results to GCS

1. Run your query
2. Click **"Save Results"** → **"GCS"**
3. Choose:
   - Destination: `gs://tf-state-soundflow-123/exports/`
   - Format: CSV, JSON, or Avro
4. Click **"Save"**

### Schedule Queries (Optional)

1. Write your query
2. Click **"Schedule"** → **"Create new scheduled query"**
3. Set:
   - Name
   - Schedule (e.g., daily at 8 AM)
   - Destination table
4. Click **"Save"**

---

## 🎯 Quick Verification Checklist

After running the pipeline, verify in UI:

### ✅ GCS Verification
- [ ] Navigate to `raw/listen_events/` - files exist
- [ ] Navigate to `raw/page_view_events/` - files exist
- [ ] Navigate to `raw/auth_events/` - files exist
- [ ] Navigate to `raw/status_change_events/` - files exist
- [ ] Check file sizes are reasonable (not 0 bytes)

### ✅ BigQuery Raw Data
- [ ] `raw.ext_listen_events` - Preview shows data
- [ ] `raw.ext_page_view_events` - Preview shows data
- [ ] `raw.ext_auth_events` - Preview shows data
- [ ] `raw.ext_status_change_events` - Preview shows data

### ✅ BigQuery Staging
- [ ] `staging_staging.stg_listens` - Query returns rows
- [ ] `staging_staging.stg_page_views` - Query returns rows

### ✅ BigQuery Marts
- [ ] `staging_marts.mart_top_songs` - Has top songs data
- [ ] `staging_marts.mart_active_users` - Has user engagement data
- [ ] `staging_marts.mart_location_analytics` - Has geographic data
- [ ] `staging_marts.mart_hourly_metrics` - Has hourly aggregates

---

## 🔑 Keyboard Shortcuts (BigQuery)

| Shortcut | Action |
|----------|--------|
| `Ctrl + Enter` | Run query |
| `Ctrl + Shift + F` | Format query |
| `Ctrl + /` | Comment/uncomment line |
| `Ctrl + Space` | Autocomplete |
| `Ctrl + S` | Save query |
| `Tab` | Indent |
| `Shift + Tab` | Outdent |

---

## 📝 Tips & Best Practices

### Query Optimization
- Use `LIMIT` when exploring data
- Select only needed columns (avoid `SELECT *`)
- Use partitioned columns in `WHERE` clause

### Cost Control
- Check "Bytes processed" before running large queries
- Use query cache (repeat queries are free)
- Set up billing alerts

### Data Validation
- Compare row counts between raw and staging
- Check for NULL values in key columns
- Verify date ranges match expected data

---

## 🆘 Troubleshooting

### "Table not found" in BigQuery
→ Check if external table exists and GCS path has data

### "Access Denied" errors
→ Verify service account has correct IAM roles

### Empty query results
→ Check if GCS folder has Parquet files

### Schema mismatch errors
→ Delete and recreate external table

---

## 📚 Additional Resources

- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Cloud Storage Documentation](https://cloud.google.com/storage/docs)
- [BigQuery SQL Reference](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)
- [Pricing Calculator](https://cloud.google.com/products/calculator)
