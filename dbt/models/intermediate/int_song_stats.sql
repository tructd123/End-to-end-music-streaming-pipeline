{{
    config(
        materialized='view',
        schema='intermediate'
    )
}}

/*
    Intermediate model: Song statistics
    - Aggregates song-level metrics from staging
*/

WITH listens AS (
    SELECT * FROM {{ ref('stg_listens') }}
),

song_stats AS (
    SELECT
        song,
        artist,
        
        -- Play counts
        COUNT(*) AS total_plays,
        COUNT(DISTINCT user_id) AS unique_listeners,
        COUNT(DISTINCT session_id) AS unique_sessions,
        
        -- User level breakdown
        COUNT(CASE WHEN subscription_level = 'paid' THEN 1 END) AS paid_plays,
        COUNT(CASE WHEN subscription_level = 'free' THEN 1 END) AS free_plays,
        ROUND(
            {{ to_numeric('COUNT(CASE WHEN subscription_level = \047paid\047 THEN 1 END)') }} / 
            NULLIF(COUNT(*), 0) * 100, 2
        ) AS paid_ratio_pct,
        
        -- Time analysis
        MIN(event_timestamp) AS first_played_at,
        MAX(event_timestamp) AS last_played_at,
        COUNT(DISTINCT event_date) AS days_with_plays,
        
        -- Time of day popularity
        COUNT(CASE WHEN time_of_day = 'Morning' THEN 1 END) AS morning_plays,
        COUNT(CASE WHEN time_of_day = 'Afternoon' THEN 1 END) AS afternoon_plays,
        COUNT(CASE WHEN time_of_day = 'Evening' THEN 1 END) AS evening_plays,
        COUNT(CASE WHEN time_of_day = 'Night' THEN 1 END) AS night_plays,
        
        -- Weekend vs weekday
        COUNT(CASE WHEN is_weekend THEN 1 END) AS weekend_plays,
        COUNT(CASE WHEN NOT is_weekend THEN 1 END) AS weekday_plays

    FROM listens
    GROUP BY song, artist
)

SELECT 
    *,
    -- Average plays per day
    ROUND({{ to_numeric('total_plays') }} / NULLIF(days_with_plays, 0), 2) AS avg_plays_per_day,
    
    -- Most popular time of day
    CASE 
        WHEN GREATEST(morning_plays, afternoon_plays, evening_plays, night_plays) = morning_plays THEN 'Morning'
        WHEN GREATEST(morning_plays, afternoon_plays, evening_plays, night_plays) = afternoon_plays THEN 'Afternoon'
        WHEN GREATEST(morning_plays, afternoon_plays, evening_plays, night_plays) = evening_plays THEN 'Evening'
        ELSE 'Night'
    END AS peak_time_of_day

FROM song_stats
