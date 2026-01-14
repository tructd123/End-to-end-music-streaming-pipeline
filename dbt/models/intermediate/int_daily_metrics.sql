{{
    config(
        materialized='view',
        schema='intermediate'
    )
}}

/*
    Intermediate model: Daily metrics
    - Aggregates daily-level KPIs
*/

WITH listens AS (
    SELECT * FROM {{ ref('stg_listens') }}
),

daily_metrics AS (
    SELECT
        event_date,
        
        -- Volume metrics
        COUNT(*) AS total_plays,
        COUNT(DISTINCT user_id) AS unique_users,
        COUNT(DISTINCT song) AS unique_songs,
        COUNT(DISTINCT artist) AS unique_artists,
        COUNT(DISTINCT session_id) AS total_sessions,
        
        -- User level breakdown
        COUNT(CASE WHEN subscription_level = 'paid' THEN 1 END) AS paid_plays,
        COUNT(CASE WHEN subscription_level = 'free' THEN 1 END) AS free_plays,
        COUNT(DISTINCT CASE WHEN subscription_level = 'paid' THEN user_id END) AS paid_users,
        COUNT(DISTINCT CASE WHEN subscription_level = 'free' THEN user_id END) AS free_users,
        
        -- Time distribution
        COUNT(CASE WHEN time_of_day = 'Morning' THEN 1 END) AS morning_plays,
        COUNT(CASE WHEN time_of_day = 'Afternoon' THEN 1 END) AS afternoon_plays,
        COUNT(CASE WHEN time_of_day = 'Evening' THEN 1 END) AS evening_plays,
        COUNT(CASE WHEN time_of_day = 'Night' THEN 1 END) AS night_plays

    FROM listens
    GROUP BY event_date
)

SELECT 
    *,
    
    -- Derived KPIs
    ROUND({{ to_numeric('total_plays') }} / NULLIF(unique_users, 0), 2) AS plays_per_user,
    ROUND({{ to_numeric('total_plays') }} / NULLIF(total_sessions, 0), 2) AS plays_per_session,
    ROUND({{ to_numeric('paid_plays') }} / NULLIF(total_plays, 0) * 100, 2) AS paid_plays_pct,
    ROUND({{ to_numeric('paid_users') }} / NULLIF(unique_users, 0) * 100, 2) AS paid_users_pct

FROM daily_metrics
ORDER BY event_date DESC
