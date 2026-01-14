{{
    config(
        materialized='table',
        schema='marts'
    )
}}

/*
    Mart: Top Songs
    - Top 100 songs ranked by total plays
    - Materialized as table for fast dashboard queries
*/

WITH song_stats AS (
    SELECT * FROM {{ ref('int_song_stats') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY total_plays DESC) AS rank,
    song,
    artist,
    total_plays,
    unique_listeners,
    unique_sessions,
    paid_plays,
    free_plays,
    paid_ratio_pct,
    first_played_at,
    last_played_at,
    days_with_plays,
    avg_plays_per_day,
    peak_time_of_day,
    
    -- Additional metrics
    ROUND({{ to_numeric('total_plays') }} / NULLIF(unique_listeners, 0), 2) AS plays_per_listener,
    
    {{ now() }} AS updated_at

FROM song_stats
ORDER BY total_plays DESC
LIMIT 100
