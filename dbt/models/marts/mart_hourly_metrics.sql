{{
    config(
        materialized='incremental',
        schema='marts',
        unique_key=['event_date', 'event_hour'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

/*
    Mart: Hourly Metrics (INCREMENTAL)
    - Hourly aggregated KPIs for time-series dashboards
    - Only processes new hours since last run
    - Merge strategy: updates existing hours, inserts new ones
*/

WITH listens AS (
    SELECT * FROM {{ ref('stg_listens') }}
    {% if is_incremental() %}
    -- Only process data from the last 2 hours to handle late-arriving data
    WHERE event_timestamp > (
        SELECT {{ timestamp_sub_hours('COALESCE(MAX(hour_timestamp), ' ~ default_timestamp() ~ ')', 2) }}
        FROM {{ this }}
    )
    {% endif %}
),

hourly_stats AS (
    SELECT
        event_date,
        event_hour,
        time_of_day,
        
        -- Volume
        COUNT(*) AS total_plays,
        COUNT(DISTINCT user_id) AS unique_users,
        COUNT(DISTINCT song) AS unique_songs,
        COUNT(DISTINCT session_id) AS total_sessions,
        
        -- User breakdown
        COUNT(CASE WHEN subscription_level = 'paid' THEN 1 END) AS paid_plays,
        COUNT(CASE WHEN subscription_level = 'free' THEN 1 END) AS free_plays

    FROM listens
    GROUP BY event_date, event_hour, time_of_day
)

SELECT
    event_date,
    event_hour,
    time_of_day,
    
    -- Create timestamp for time-series
    {{ date_add_hours('event_date', 'event_hour') }} AS hour_timestamp,
    
    total_plays,
    unique_users,
    unique_songs,
    total_sessions,
    paid_plays,
    free_plays,
    
    -- Derived metrics
    ROUND({{ to_numeric('total_plays') }} / NULLIF(unique_users, 0), 2) AS plays_per_user,
    ROUND({{ to_numeric('total_plays') }} / NULLIF(total_sessions, 0), 2) AS plays_per_session,
    ROUND({{ to_numeric('paid_plays') }} / NULLIF(total_plays, 0) * 100, 2) AS paid_ratio_pct,
    
    {{ now() }} AS updated_at

FROM hourly_stats
ORDER BY event_date DESC, event_hour DESC
