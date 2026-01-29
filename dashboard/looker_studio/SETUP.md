# Looker Studio Setup Guide

## Prerequisites
- Google account
- Access to BigQuery project: `graphic-boulder-483814-g7`
- BigQuery data (run dbt pipeline first)

## Step 1: Create New Report

1. Go to [Looker Studio](https://lookerstudio.google.com/)
2. Click **"Create"** → **"Report"**
3. Select **"BigQuery"** as data source

## Step 2: Connect to BigQuery

1. Select project: `graphic-boulder-483814-g7`
2. Select dataset: `staging_marts`
3. Add these tables as data sources:
   - `mart_top_songs`
   - `mart_daily_summary`
   - `mart_active_users`
   - `mart_hourly_metrics`
   - `mart_location_analytics`

## Step 3: Create Dashboard Pages

### Page 1: Executive Overview

**KPI Scorecards (top row):**
```
┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
│  Total Plays    │  Active Users   │  Paid Users %   │  Unique Songs   │
│    Today        │    Today        │                 │    Played       │
└─────────────────┴─────────────────┴─────────────────┴─────────────────┘
```

**Charts:**
1. **Daily Plays Trend** (Line Chart)
   - Data source: `mart_daily_summary`
   - Dimension: `event_date`
   - Metric: `total_plays`
   - Date range: Last 30 days

2. **Paid vs Free Distribution** (Pie Chart)
   - Data source: `mart_daily_summary`
   - Dimension: Create calculated field
   - Metrics: `paid_plays`, `free_plays`

3. **Top 5 Songs Today** (Table)
   - Data source: `mart_top_songs`
   - Dimensions: `rank`, `song`, `artist`
   - Metrics: `total_plays`
   - Filter: `rank <= 5`

### Page 2: Music Analytics

**Charts:**
1. **Top 10 Songs** (Bar Chart)
   - Data source: `mart_top_songs`
   - Dimension: `song`
   - Metric: `total_plays`
   - Sort: Descending
   - Filter: `rank <= 10`

2. **Top 10 Artists** (Bar Chart)
   - Data source: `mart_top_artists`
   - Dimension: `artist`
   - Metric: `total_plays`

3. **Songs Leaderboard** (Table with bars)
   - Data source: `mart_top_songs`
   - Columns: rank, song, artist, total_plays, unique_listeners, paid_ratio_pct
   - Enable data bars on total_plays

4. **Play Time Distribution** (Pie Chart)
   - Based on `peak_time_of_day` from `mart_top_songs`

### Page 3: User Engagement

**Charts:**
1. **Engagement Tier Distribution** (Donut Chart)
   - Data source: `mart_active_users`
   - Dimension: `engagement_tier`
   - Metric: COUNT of users

2. **User Activity by Time** (Heatmap)
   - Data source: `mart_active_users`
   - Dimensions: `favorite_time`, `preferred_days`

3. **Top Users** (Table)
   - Data source: `mart_active_users`
   - Columns: full_name, total_plays, engagement_tier, active_days

4. **User Location Map** (Geo Map)
   - Data source: `mart_location_analytics`
   - Geo dimension: `state`
   - Metric: `total_plays`

### Page 4: Hourly Trends

**Charts:**
1. **Plays by Hour Today** (Area Chart)
   - Data source: `mart_hourly_metrics`
   - Dimension: `hour_of_day`
   - Metric: `hourly_plays`
   - Filter: `event_date = TODAY()`

2. **Peak Hours Analysis** (Bar Chart)
   - Data source: `mart_hourly_metrics`
   - Dimension: `hour_of_day`
   - Metric: AVG(`hourly_plays`)

## Step 4: Add Filters & Interactivity

1. **Date Range Control**
   - Add to every page
   - Default: Last 7 days

2. **Dropdown Filters**
   - Filter by state/location
   - Filter by user level (paid/free)

## Step 5: Styling

**Theme:**
- Primary Color: `#1DB954` (Spotify green)
- Background: `#191414` (Dark theme)
- Text: White

**Layout:**
- Use grid alignment
- Consistent margins (16px)
- Card-style containers for charts

## Custom SQL Queries

For advanced charts, create Custom Queries in BigQuery data source:

### Query 1: Daily Growth Rate
```sql
SELECT
    event_date,
    total_plays,
    unique_users,
    LAG(total_plays) OVER (ORDER BY event_date) as prev_day_plays,
    ROUND((total_plays - LAG(total_plays) OVER (ORDER BY event_date)) * 100.0 
          / NULLIF(LAG(total_plays) OVER (ORDER BY event_date), 0), 2) as growth_rate_pct
FROM `graphic-boulder-483814-g7.staging_marts.mart_daily_summary`
ORDER BY event_date DESC
LIMIT 30
```

### Query 2: User Retention Cohort
```sql
SELECT
    engagement_tier,
    COUNT(*) as user_count,
    AVG(total_plays) as avg_plays,
    AVG(active_days) as avg_active_days
FROM `graphic-boulder-483814-g7.staging_marts.mart_active_users`
GROUP BY engagement_tier
ORDER BY user_count DESC
```

### Query 3: Hourly Comparison (Today vs Yesterday)
```sql
SELECT
    hour_of_day,
    SUM(CASE WHEN event_date = CURRENT_DATE() THEN hourly_plays ELSE 0 END) as today_plays,
    SUM(CASE WHEN event_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN hourly_plays ELSE 0 END) as yesterday_plays
FROM `graphic-boulder-483814-g7.staging_marts.mart_hourly_metrics`
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY hour_of_day
ORDER BY hour_of_day
```

## Sharing

1. Click **"Share"** button
2. Options:
   - Invite specific users
   - Get shareable link
   - Schedule email delivery
   - Embed in website

## Tips

- Enable **Auto-refresh** for real-time data
- Use **Calculated Fields** for custom metrics
- Set up **Scheduled Data Refresh** in BigQuery connection settings
