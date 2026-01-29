# SoundFlow Dashboard

## Overview

This directory contains dashboard configurations and SQL queries for visualizing SoundFlow music streaming analytics.

## Available Data Sources (BigQuery)

| Table | Description | Update Frequency |
|-------|-------------|------------------|
| `staging_marts.mart_top_songs` | Top 100 songs by plays | Daily |
| `staging_marts.mart_top_artists` | Top artists rankings | Daily |
| `staging_marts.mart_active_users` | User engagement metrics | Daily |
| `staging_marts.mart_daily_summary` | Daily KPIs | Incremental |
| `staging_marts.mart_hourly_metrics` | Hourly trends | Incremental |
| `staging_marts.mart_location_analytics` | Geographic analytics | Daily |

## Dashboard Options

### Option 1: Looker Studio (Recommended - Free)

**Pros:**
- Free with Google account
- Native BigQuery integration
- No infrastructure to manage
- Shareable via link

**Setup:**
1. Go to [Looker Studio](https://lookerstudio.google.com/)
2. Create New Report
3. Add BigQuery data source
4. Select project: `graphic-boulder-483814-g7`
5. Choose tables from `staging_marts` dataset

See [looker_studio/SETUP.md](looker_studio/SETUP.md) for detailed instructions.

### Option 2: Metabase (Self-hosted)

**Pros:**
- More customization options
- Embedded analytics
- SQL-native interface

**Setup:**
```bash
cd dashboard/metabase
docker-compose up -d
```

Access at http://localhost:3030

See [metabase/SETUP.md](metabase/SETUP.md) for detailed instructions.

## Dashboard Pages

### 1. Executive Overview
- Total plays today/this week/this month
- Active users trend
- Revenue indicators (paid vs free)
- Top 5 songs

### 2. Music Analytics
- Top songs chart
- Top artists leaderboard
- Song popularity over time
- Genre distribution (if available)

### 3. User Engagement
- User activity heatmap
- Engagement tier distribution
- User retention metrics
- Geographic distribution

### 4. Real-time Monitoring
- Hourly plays trend
- Current active users
- System health metrics

## Quick Links

- BigQuery Console: https://console.cloud.google.com/bigquery?project=graphic-boulder-483814-g7
- Looker Studio: https://lookerstudio.google.com/
