{{
    config(
        materialized='view',
        schema='intermediate'
    )
}}

/*
    Intermediate model: User activity
    - Aggregates user-level metrics from staging
*/

WITH listens AS (
    SELECT * FROM {{ ref('stg_listens') }}
),

user_stats AS (
    SELECT
        user_id,
        
        -- User info (take most recent)
        MAX(full_name) AS full_name,
        MAX(subscription_level) AS current_level,
        MAX(city) AS city,
        MAX(state) AS state,
        
        -- Listening activity
        COUNT(*) AS total_plays,
        COUNT(DISTINCT song) AS unique_songs,
        COUNT(DISTINCT artist) AS unique_artists,
        COUNT(DISTINCT session_id) AS total_sessions,
        
        -- Time range
        MIN(event_timestamp) AS first_listen_at,
        MAX(event_timestamp) AS last_listen_at,
        COUNT(DISTINCT event_date) AS active_days,
        
        -- Calculate listening span in days
        {% if target.type == 'postgres' %}
        EXTRACT(DAY FROM (MAX(event_timestamp) - MIN(event_timestamp))) + 1 AS listening_span_days,
        {% elif target.type == 'bigquery' %}
        DATE_DIFF(DATE(MAX(event_timestamp)), DATE(MIN(event_timestamp)), DAY) + 1 AS listening_span_days,
        {% endif %}
        
        -- Time preferences
        COUNT(CASE WHEN time_of_day = 'Morning' THEN 1 END) AS morning_plays,
        COUNT(CASE WHEN time_of_day = 'Afternoon' THEN 1 END) AS afternoon_plays,
        COUNT(CASE WHEN time_of_day = 'Evening' THEN 1 END) AS evening_plays,
        COUNT(CASE WHEN time_of_day = 'Night' THEN 1 END) AS night_plays,
        
        -- Weekend activity
        COUNT(CASE WHEN is_weekend THEN 1 END) AS weekend_plays,
        COUNT(CASE WHEN NOT is_weekend THEN 1 END) AS weekday_plays

    FROM listens
    GROUP BY user_id
)

SELECT 
    *,
    
    -- Derived metrics
    ROUND({{ to_numeric('total_plays') }} / NULLIF(active_days, 0), 2) AS avg_plays_per_active_day,
    ROUND({{ to_numeric('unique_songs') }} / NULLIF(total_plays, 0) * 100, 2) AS song_diversity_pct,
    ROUND({{ to_numeric('total_plays') }} / NULLIF(total_sessions, 0), 2) AS avg_plays_per_session,
    
    -- Favorite time of day
    CASE 
        WHEN GREATEST(morning_plays, afternoon_plays, evening_plays, night_plays) = morning_plays THEN 'Morning'
        WHEN GREATEST(morning_plays, afternoon_plays, evening_plays, night_plays) = afternoon_plays THEN 'Afternoon'
        WHEN GREATEST(morning_plays, afternoon_plays, evening_plays, night_plays) = evening_plays THEN 'Evening'
        ELSE 'Night'
    END AS favorite_time,
    
    -- Weekend preference
    CASE 
        WHEN weekend_plays > weekday_plays THEN 'Weekend'
        ELSE 'Weekday'
    END AS preferred_days,
    
    -- User engagement tier
    CASE 
        WHEN total_plays >= 1000 THEN 'Power User'
        WHEN total_plays >= 100 THEN 'Active'
        WHEN total_plays >= 10 THEN 'Casual'
        ELSE 'New'
    END AS engagement_tier

FROM user_stats
