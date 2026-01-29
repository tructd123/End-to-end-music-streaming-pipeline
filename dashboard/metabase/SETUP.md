# Metabase Setup Guide

## Prerequisites
- Docker & Docker Compose
- BigQuery Service Account key (`dbt-sa-key.json`)

## Step 1: Start Metabase

```bash
cd dashboard/metabase
docker-compose up -d
```

Wait 1-2 minutes for startup, then access: http://localhost:3030

## Step 2: Initial Setup

1. Create admin account
2. Select language & timezone (Asia/Ho_Chi_Minh)
3. Skip "Add your data" for now

## Step 3: Connect to BigQuery

1. Go to **Admin** → **Databases** → **Add database**
2. Select **BigQuery**
3. Configure:
   - **Display name**: SoundFlow BigQuery
   - **Project ID**: `graphic-boulder-483814-g7`
   - **Dataset ID**: `staging_marts`
   - **Service account JSON**: Upload `credentials/dbt-sa-key.json`

4. Click **Save**

## Step 4: Sync Schema

1. After adding database, click **Sync database schema now**
2. Wait for sync to complete
3. You should see 6 tables:
   - mart_active_users
   - mart_daily_summary
   - mart_hourly_metrics
   - mart_location_analytics
   - mart_top_artists
   - mart_top_songs

## Step 5: Create Dashboard

### Dashboard 1: Executive Overview

Click **New** → **Dashboard** → Name: "SoundFlow Executive Dashboard"

#### Card 1: Total Plays Today (Scorecard)
```sql
SELECT SUM(total_plays) as total_plays
FROM staging_marts.mart_daily_summary
WHERE event_date = CURRENT_DATE()
```

#### Card 2: Active Users (Scorecard)
```sql
SELECT COUNT(*) as active_users
FROM staging_marts.mart_active_users
WHERE is_active = TRUE
```

#### Card 3: Daily Plays Trend (Line Chart)
```sql
SELECT 
    event_date,
    total_plays,
    unique_users
FROM staging_marts.mart_daily_summary
ORDER BY event_date DESC
LIMIT 30
```

#### Card 4: Top 10 Songs (Bar Chart)
```sql
SELECT 
    song,
    artist,
    total_plays
FROM staging_marts.mart_top_songs
WHERE rank <= 10
ORDER BY total_plays DESC
```

#### Card 5: Engagement Tiers (Pie Chart)
```sql
SELECT 
    engagement_tier,
    COUNT(*) as user_count
FROM staging_marts.mart_active_users
GROUP BY engagement_tier
```

### Dashboard 2: Music Analytics

#### Card 1: Songs Leaderboard (Table)
```sql
SELECT 
    rank,
    song,
    artist,
    total_plays,
    unique_listeners,
    paid_ratio_pct,
    last_played_at
FROM staging_marts.mart_top_songs
ORDER BY rank
LIMIT 50
```

#### Card 2: Artists Leaderboard (Table)
```sql
SELECT 
    artist,
    total_plays,
    unique_songs,
    unique_listeners,
    avg_plays_per_song
FROM staging_marts.mart_top_artists
ORDER BY total_plays DESC
LIMIT 20
```

#### Card 3: Hourly Distribution (Area Chart)
```sql
SELECT 
    hour_of_day,
    SUM(hourly_plays) as total_plays,
    AVG(active_users_in_hour) as avg_active_users
FROM staging_marts.mart_hourly_metrics
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY hour_of_day
ORDER BY hour_of_day
```

### Dashboard 3: User Analytics

#### Card 1: User Activity Heatmap
```sql
SELECT 
    favorite_time,
    preferred_days,
    COUNT(*) as user_count
FROM staging_marts.mart_active_users
GROUP BY favorite_time, preferred_days
```

#### Card 2: Top Users (Table)
```sql
SELECT 
    full_name,
    current_level,
    total_plays,
    unique_songs,
    engagement_tier,
    active_days
FROM staging_marts.mart_active_users
ORDER BY total_plays DESC
LIMIT 100
```

#### Card 3: Geographic Distribution (Map)
```sql
SELECT 
    state,
    total_plays,
    unique_users
FROM staging_marts.mart_location_analytics
ORDER BY total_plays DESC
```

## Step 6: Set Up Auto-Refresh

1. Go to Dashboard settings (gear icon)
2. Enable **Auto-refresh**
3. Set interval: 5 minutes

## Step 7: Create Alerts (Optional)

1. Go to question/card
2. Click bell icon
3. Set conditions:
   - "Alert when total_plays drops below X"
   - "Alert when error rate exceeds Y%"

## Embedding Metabase

For embedding in other applications:

1. Go to **Admin** → **Embedding**
2. Enable **Public Sharing** or **Embedding**
3. Get embed code:
   ```html
   <iframe 
       src="http://localhost:3030/public/dashboard/xxx"
       width="100%" 
       height="600">
   </iframe>
   ```

## Backup

```bash
# Backup Metabase data
docker exec metabase-postgres pg_dump -U metabase metabase > metabase_backup.sql

# Restore
docker exec -i metabase-postgres psql -U metabase metabase < metabase_backup.sql
```

## Troubleshooting

### BigQuery Connection Failed
- Check service account has `BigQuery Data Viewer` role
- Verify JSON key file is valid

### Slow Queries
- Add indexes in BigQuery (partition by date)
- Use caching in Metabase

### Container Won't Start
```bash
docker-compose down
docker volume rm metabase_metabase-data metabase_postgres-data
docker-compose up -d
```
