# SoundFlow - Local PostgreSQL Setup

Hướng dẫn chạy pipeline hoàn chỉnh trên local với PostgreSQL.

## 📋 Prerequisites

- Docker & Docker Compose
- 8GB RAM minimum
- Ports available: 5432, 3000, 8080, 8081, 9092

## 🚀 Quick Start

### 1. Start All Services

```bash
docker-compose -f docker-compose.local.yml up -d
```

Services khởi động:
- ✅ PostgreSQL (port 5432)
- ✅ Adminer UI (port 8081)
- ✅ Redpanda (port 9092)
- ✅ Redpanda Console (port 8080)
- ✅ EventSim (event generator)
- ✅ Spark Streaming (writes to PostgreSQL every 30s)
- ✅ Dagster (port 3000)

### 2. Monitor Services

**Xem logs:**
```bash
# Tất cả services
docker-compose -f docker-compose.local.yml logs -f

# Chỉ Spark
docker-compose -f docker-compose.local.yml logs -f spark-streaming

# Chỉ EventSim
docker-compose -f docker-compose.local.yml logs -f eventsim
```

**Check Redpanda topics:**
```bash
docker exec -it redpanda rpk topic list
docker exec -it redpanda rpk topic consume listen_events --num 10
```

### 3. Access Web UIs

| Service | URL | Description |
|---------|-----|-------------|
| **Adminer** | http://localhost:8081 | PostgreSQL Admin UI |
| **Redpanda Console** | http://localhost:8080 | Kafka/Topics Monitor |
| **Dagster** | http://localhost:3000 | Workflow Orchestration |

**Login Adminer:**
- System: `PostgreSQL`
- Server: `postgres`
- Username: `soundflow`
- Password: `soundflow123`
- Database: `soundflow`

### 4. Check PostgreSQL Data

```bash
# Connect to PostgreSQL
docker exec -it postgres psql -U soundflow -d soundflow

# Check tables
\dt raw.*

# Count records
SELECT COUNT(*) FROM raw.listen_events;
SELECT COUNT(*) FROM raw.page_view_events;
SELECT COUNT(*) FROM raw.auth_events;

# View latest listen events
SELECT event_timestamp, first_name, last_name, song, artist, city, state 
FROM raw.listen_events 
ORDER BY event_timestamp DESC 
LIMIT 10;
```

### 5. Run dbt Transformations

```bash
# Enter dbt container (if running)
docker exec -it dagster bash

# Or run dbt locally
cd dbt
dbt debug --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

Check transformed data:
```sql
-- Staging tables
SELECT * FROM staging.stg_listen_events LIMIT 10;

-- Marts
SELECT * FROM marts.mart_top_songs LIMIT 20;
SELECT * FROM marts.mart_active_users LIMIT 20;
SELECT * FROM marts.mart_location_analytics;
```

## 🔍 Troubleshooting

**Spark không ghi dữ liệu:**
```bash
# Check Spark logs
docker logs spark-streaming

# Restart Spark
docker-compose -f docker-compose.local.yml restart spark-streaming
```

**PostgreSQL connection refused:**
```bash
# Check PostgreSQL is running
docker exec -it postgres pg_isready -U soundflow

# Check network
docker network inspect data_streaming_pipeline_streaming-network
```

**EventSim stopped:**
```bash
# EventSim generates 7 days of historical data then stops in continuous mode
# Check logs to see if it's still generating
docker logs eventsim

# Restart to generate more data
docker-compose -f docker-compose.local.yml restart eventsim
```

## 🛑 Stop Services

```bash
# Stop all
docker-compose -f docker-compose.local.yml down

# Stop and remove volumes (⚠️ deletes all data)
docker-compose -f docker-compose.local.yml down -v
```

## 📊 Data Flow

```
EventSim 
  ↓ (JSON events)
Redpanda (4 topics: listen_events, page_view_events, auth_events, status_change_events)
  ↓ (Kafka consumer)
Spark Streaming (micro-batch every 30s)
  ↓ (JDBC write)
PostgreSQL raw schema
  ↓ (dbt transformations)
PostgreSQL staging schema
  ↓ (dbt aggregations)
PostgreSQL marts schema
  ↓
Dashboard / Analytics
```

## 🎯 Next Steps

1. ✅ Verify data in PostgreSQL
2. ✅ Run dbt models
3. ✅ Connect to Power BI / Metabase / Grafana
4. 🚀 Deploy to cloud (GCP) when ready
