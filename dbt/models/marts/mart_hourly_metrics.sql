{{
    config(
        materialized='table',
        partition_by={
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

-- MART: Hourly Metrics
-- Time-series data for real-time dashboard

WITH hourly_listens AS (
    SELECT
        event_date,
        hour,
        time_of_day,
        day_of_week,
        COUNT(*) as listen_count,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT song) as unique_songs,
        COUNT(DISTINCT session_id) as unique_sessions,
        SUM(duration_seconds) / 3600 as total_listen_hours
        
    FROM {{ ref('stg_listens') }}
    GROUP BY 1, 2, 3, 4
),

hourly_page_views AS (
    SELECT
        event_date,
        hour,
        COUNT(*) as page_view_count,
        COUNT(DISTINCT user_id) as unique_page_viewers
        
    FROM {{ ref('stg_page_views') }}
    GROUP BY 1, 2
)

SELECT
    l.event_date,
    l.hour,
    l.time_of_day,
    l.day_of_week,
    l.listen_count,
    l.unique_users as unique_listeners,
    l.unique_songs,
    l.unique_sessions,
    ROUND(l.total_listen_hours, 2) as total_listen_hours,
    COALESCE(p.page_view_count, 0) as page_view_count,
    COALESCE(p.unique_page_viewers, 0) as unique_page_viewers,
    -- Derived metrics
    ROUND(l.listen_count / NULLIF(l.unique_users, 0), 2) as avg_listens_per_user,
    TIMESTAMP(DATETIME(l.event_date, TIME(l.hour, 0, 0))) as hour_timestamp

FROM hourly_listens l
LEFT JOIN hourly_page_views p 
    ON l.event_date = p.event_date AND l.hour = p.hour
ORDER BY l.event_date DESC, l.hour DESC
