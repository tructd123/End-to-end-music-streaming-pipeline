# 🔄 Dagster Orchestration Guide

A comprehensive guide to using Dagster for automating the SoundFlow data pipeline.

---

## 📋 Table of Contents

1. [Overview](#-overview)
2. [Starting Dagster](#-starting-dagster)
3. [UI Navigation](#-ui-navigation)
4. [Running Jobs](#-running-jobs)
5. [Managing Schedules](#-managing-schedules)
6. [Viewing Assets](#-viewing-assets)
7. [Monitoring Runs](#-monitoring-runs)
8. [Troubleshooting](#-troubleshooting)

---

## 🎯 Overview

Dagster orchestrates the entire SoundFlow pipeline:

```
┌─────────────────────────────────────────────────────────────────┐
│                    DAGSTER ORCHESTRATION                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐ │
│  │  EventSim   │───▶│   Kafka     │───▶│  kafka_to_gcs       │ │
│  │  Generate   │    │   Health    │    │  Transfer           │ │
│  └─────────────┘    └─────────────┘    └──────────┬──────────┘ │
│                                                    │            │
│                                                    ▼            │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                      GCS Data Freshness                     ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                    │            │
│                                                    ▼            │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐ │
│  │  Staging    │───▶│Intermediate │───▶│      Marts          │ │
│  │  Models     │    │  Models     │    │     Models          │ │
│  └─────────────┘    └─────────────┘    └──────────┬──────────┘ │
│                                                    │            │
│                                                    ▼            │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                      dbt Tests                              ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Available Components

| Component | Count | Description |
|-----------|-------|-------------|
| **Assets** | 16 | Data assets (ETL, dbt models, validations) |
| **Jobs** | 7 | Executable pipelines |
| **Schedules** | 6 | Automated triggers |
| **Sensors** | 1 | Event-driven triggers |

---

## 🚀 Starting Dagster

### Option 1: Using PowerShell Script

```powershell
# Navigate to project root
cd E:\Individual\Data_streaming_pipeline

# Load environment variables and start Dagster
$env:GCP_PROJECT = "graphic-boulder-483814-g7"
$env:GCS_BUCKET = "tf-state-soundflow-123"
$env:GOOGLE_APPLICATION_CREDENTIALS = "E:\Individual\Data_streaming_pipeline\credentials\dbt-sa-key.json"
$env:DAGSTER_HOME = "E:\Individual\Data_streaming_pipeline\dagster\dagster_home"

cd dagster
dagster dev -m dagster_pipeline -p 3000
```

### Option 2: Using Docker (Production)

```powershell
docker-compose up -d dagster
```

### Access UI

Open browser: **http://localhost:3000**

---

## 🖥️ UI Navigation

### Main Sections

| Section | Location | Purpose |
|---------|----------|---------|
| **Overview** | Top nav | Dashboard with recent runs |
| **Assets** | Left sidebar | View all data assets and lineage |
| **Jobs** | Left sidebar | View and run jobs |
| **Schedules** | Left sidebar → Automation | Manage scheduled runs |
| **Sensors** | Left sidebar → Automation | Manage event triggers |
| **Runs** | Left sidebar | View run history |

### Navigation Tips

1. **Global Asset Lineage**: Assets → Click "View global asset lineage"
2. **Filter Assets**: Use the search bar to filter by name/group
3. **Quick Run**: Click any asset → "Materialize" button

---

## ▶️ Running Jobs

### Available Jobs

| Job Name | Purpose | When to Use |
|----------|---------|-------------|
| `full_pipeline` | Complete pipeline (EventSim → dbt) | Daily full refresh |
| `etl_kafka_to_gcs` | Kafka → GCS transfer only | After generating events |
| `dbt_full_pipeline` | All dbt transformations | After new data in GCS |
| `dbt_marts_only` | Incremental marts update | Frequent updates |
| `dbt_local_pipeline` | Local PostgreSQL dbt | Development |
| `bigquery_validation` | Validate BQ data | Data quality checks |
| `infrastructure_check` | Check GCS/Spark | Debugging |

### Run Job from UI

1. Go to **Jobs** in left sidebar
2. Click on job name (e.g., `dbt_full_pipeline`)
3. Click **"Launchpad"** button (top right)
4. Review config (usually default is fine)
5. Click **"Launch Run"**

### Run Job from Terminal

```powershell
# Ensure environment is set
$env:GCP_PROJECT = "graphic-boulder-483814-g7"
$env:DAGSTER_HOME = "E:\Individual\Data_streaming_pipeline\dagster\dagster_home"

# Execute job
dagster job execute -m dagster_pipeline -j dbt_full_pipeline
```

### Run Specific Assets

```powershell
# Materialize specific asset
dagster asset materialize -m dagster_pipeline --select kafka_to_gcs_transfer
```

---

## ⏰ Managing Schedules

### Available Schedules

| Schedule | Cron | Frequency | Job |
|----------|------|-----------|-----|
| `hourly_etl_transfer` | `30 * * * *` | Every hour at :30 | etl_kafka_to_gcs |
| `hourly_full_pipeline` | `0 * * * *` | Every hour at :00 | dbt_full_pipeline |
| `daily_full_pipeline_run` | `0 1 * * *` | Daily at 1 AM | full_pipeline |
| `frequent_marts_update` | `*/15 * * * *` | Every 15 minutes | dbt_marts_only |
| `bigquery_validation_check` | `*/30 * * * *` | Every 30 minutes | bigquery_validation |

### Enable/Disable Schedule in UI

1. Go to **Automation** → **Schedules**
2. Find the schedule you want
3. Toggle the switch to **ON** or **OFF**

### Enable Schedule via Terminal

```powershell
# Start schedule
dagster schedule start -m dagster_pipeline hourly_full_pipeline

# Stop schedule
dagster schedule stop -m dagster_pipeline hourly_full_pipeline

# List all schedules
dagster schedule list -m dagster_pipeline
```

### Recommended Schedule Setup

For typical usage:

| Schedule | Recommended State |
|----------|-------------------|
| `daily_full_pipeline_run` | ✅ ON |
| `hourly_full_pipeline` | ⚠️ ON (if need hourly updates) |
| `frequent_marts_update` | ❌ OFF (only for real-time needs) |
| `bigquery_validation_check` | ✅ ON |

---

## 📊 Viewing Assets

### Asset Groups

| Group | Assets | Description |
|-------|--------|-------------|
| **etl** | 3 | Kafka health, EventSim, GCS transfer |
| **dbt** | 5 | Staging, intermediate, marts, tests, docs |
| **bigquery** | 6 | Raw data checks, mart validations |
| **spark** | 2 | Job status, GCS freshness |

### View Asset Lineage

1. Go to **Assets**
2. Click **"View global asset lineage"** (top right)
3. See the dependency graph:

```
kafka_health_check
       │
       ├──────────────────┐
       ▼                  ▼
eventsim_generate    kafka_to_gcs_transfer
                          │
                          ▼
                   gcs_data_freshness
                          │
                          ▼
                  dbt_staging_models
                          │
                          ▼
               dbt_intermediate_models
                          │
                          ▼
                   dbt_marts_models
                          │
                          ▼
                   dbt_test_results
```

### Materialize Assets

**Single asset:**
1. Click on asset
2. Click **"Materialize"** button

**Multiple assets:**
1. Select assets (checkbox)
2. Click **"Materialize selected"**

**All upstream assets:**
1. Click on asset
2. Click dropdown arrow on Materialize
3. Select **"Materialize with upstream"**

---

## 📈 Monitoring Runs

### View Run History

1. Go to **Runs** in left sidebar
2. See list of all runs with status:
   - 🟢 **Success** - Completed successfully
   - 🔴 **Failed** - Error occurred
   - 🟡 **In Progress** - Currently running
   - ⚪ **Canceled** - Manually stopped

### Run Details

Click on any run to see:

| Tab | Information |
|-----|-------------|
| **Gantt** | Timeline of steps |
| **Logs** | Detailed execution logs |
| **Compute Logs** | Stdout/stderr output |

### Filter Runs

Use filters to find runs:
- By **Job** name
- By **Status** (success/failed)
- By **Date range**
- By **Tags**

### Re-run Failed Jobs

1. Go to failed run
2. Click **"Re-execute"** dropdown
3. Choose:
   - **All steps** - Re-run everything
   - **From failure** - Resume from failed step

---

## 🔧 Configuration

### Environment Variables

Dagster reads from `.env` file in dagster folder:

```env
# GCP Settings
GCP_PROJECT=graphic-boulder-483814-g7
GCP_REGION=asia-southeast1
GOOGLE_APPLICATION_CREDENTIALS=../credentials/dbt-sa-key.json
GCS_BUCKET=tf-state-soundflow-123

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# EventSim Settings
EVENTSIM_NUSERS=500
EVENTSIM_FROM_DAYS=30
EVENTSIM_TO_DAYS=0
```

### dagster.yaml Configuration

Located at `dagster_home/dagster.yaml`:

```yaml
# Run storage
storage:
  sqlite:
    base_dir: ./storage

# Schedule storage
scheduler:
  module: dagster.core.scheduler
  class: DagsterDaemonScheduler

# Sensor settings
sensors:
  use_threads: true
  num_workers: 4
```

---

## 🛠️ Troubleshooting

### Common Issues

#### 1. "Module not found: dagster"

```powershell
# Install dagster
pip install dagster dagster-webserver
```

#### 2. "GCP_PROJECT env var required"

```powershell
# Set environment variables before starting
$env:GCP_PROJECT = "graphic-boulder-483814-g7"
```

#### 3. Run stuck in "Starting"

```powershell
# Check if daemon is running
# Restart Dagster
Ctrl+C
dagster dev -m dagster_pipeline -p 3000
```

#### 4. Schedule not triggering

1. Check schedule is **ON** in UI
2. Verify daemon is running (check terminal output)
3. Check cron expression is correct

#### 5. Asset materialization failed

1. Go to **Runs** → Find failed run
2. Check **Logs** tab for error
3. Check **Compute Logs** for Python errors
4. Fix issue and re-run

### Debug Commands

```powershell
# Check Dagster definitions
python -c "from dagster_pipeline import defs; print(defs)"

# Validate definitions
dagster definitions validate -m dagster_pipeline

# List all jobs
dagster job list -m dagster_pipeline

# List all assets
dagster asset list -m dagster_pipeline
```

### View Logs

```powershell
# Dagster logs location
E:\Individual\Data_streaming_pipeline\dagster\dagster_home\logs\

# Or view in UI: Runs → Select run → Logs tab
```

---

## 📚 Quick Reference

### Keyboard Shortcuts (UI)

| Shortcut | Action |
|----------|--------|
| `G` then `A` | Go to Assets |
| `G` then `J` | Go to Jobs |
| `G` then `R` | Go to Runs |
| `/` | Open search |

### Useful Terminal Commands

```powershell
# Start Dagster dev server
dagster dev -m dagster_pipeline -p 3000

# Execute job
dagster job execute -m dagster_pipeline -j <job_name>

# Materialize asset
dagster asset materialize -m dagster_pipeline --select <asset_name>

# Start/stop schedule
dagster schedule start -m dagster_pipeline <schedule_name>
dagster schedule stop -m dagster_pipeline <schedule_name>

# Check schedule status
dagster schedule list -m dagster_pipeline
```

---

## 🎯 Typical Workflows

### Daily Operations

1. **Morning**: Check overnight `daily_full_pipeline_run` results
2. **Monitor**: Keep `bigquery_validation_check` running
3. **Ad-hoc**: Run `dbt_marts_only` after manual data updates

### After Adding New Data

```
1. Generate events    → docker-compose up eventsim
2. Transfer to GCS    → Run `etl_kafka_to_gcs` job
3. Transform data     → Run `dbt_full_pipeline` job
4. Validate           → Run `bigquery_validation` job
```

### Development Workflow

```
1. Make code changes
2. Test locally      → Run `dbt_local_pipeline` job
3. Deploy to prod    → Run `dbt_full_pipeline` job
4. Monitor           → Check Runs for success
```

---

## 📞 Resources

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster Concepts](https://docs.dagster.io/concepts)
- [dbt + Dagster Integration](https://docs.dagster.io/integrations/dbt)
- [Scheduling Guide](https://docs.dagster.io/concepts/automation/schedules)
