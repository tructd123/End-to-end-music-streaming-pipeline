{{
    config(
        materialized='incremental',
        unique_key='user_id',
        partition_by={
            "field": "last_active_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

-- Intermediate model: User activity statistics
-- Aggregates all user activities

WITH listen_stats AS (
    SELECT
        user_id,
        first_name,
        last_name,
        full_name,
        gender,
        location,
        subscription_level,
        COUNT(*) as total_listens,
        COUNT(DISTINCT song) as unique_songs,
        COUNT(DISTINCT artist) as unique_artists,
        COUNT(DISTINCT session_id) as total_sessions,
        SUM(duration_seconds) as total_listen_time_seconds,
        MIN(event_timestamp) as first_listen_at,
        MAX(event_timestamp) as last_listen_at,
        MAX(event_date) as last_active_date
    FROM {{ ref('stg_listens') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

page_view_stats AS (
    SELECT
        user_id,
        COUNT(*) as total_page_views,
        COUNT(DISTINCT page) as unique_pages_visited
    FROM {{ ref('stg_page_views') }}
    GROUP BY 1
),

auth_stats AS (
    SELECT
        user_id,
        COUNT(*) as total_auth_events,
        COUNTIF(is_success) as successful_logins
    FROM {{ ref('stg_auth') }}
    GROUP BY 1
)

SELECT
    l.user_id,
    l.first_name,
    l.last_name,
    l.full_name,
    l.gender,
    l.location,
    l.subscription_level,
    l.total_listens,
    l.unique_songs,
    l.unique_artists,
    l.total_sessions,
    ROUND(l.total_listen_time_seconds / 3600, 2) as total_listen_hours,
    l.first_listen_at,
    l.last_listen_at,
    l.last_active_date,
    COALESCE(p.total_page_views, 0) as total_page_views,
    COALESCE(p.unique_pages_visited, 0) as unique_pages_visited,
    COALESCE(a.total_auth_events, 0) as total_auth_events,
    COALESCE(a.successful_logins, 0) as successful_logins,
    -- Derived metrics
    ROUND(l.total_listens / NULLIF(l.total_sessions, 0), 2) as avg_listens_per_session,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), l.last_listen_at, HOUR) as hours_since_last_activity

FROM listen_stats l
LEFT JOIN page_view_stats p ON l.user_id = p.user_id
LEFT JOIN auth_stats a ON l.user_id = a.user_id

{% if is_incremental() %}
WHERE l.last_listen_at > (SELECT MAX(last_listen_at) FROM {{ this }})
{% endif %}
