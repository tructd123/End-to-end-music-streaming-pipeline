{{
    config(
        materialized='table',
        partition_by={
            "field": "last_played_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

-- MART: Top Songs Dashboard
-- Answers: "Bài hát nào phổ biến nhất?"

WITH ranked_songs AS (
    SELECT
        song_id,
        song,
        artist,
        total_plays,
        unique_listeners,
        unique_sessions,
        avg_duration_seconds,
        total_listen_time_hours,
        first_played_at,
        last_played_at,
        last_played_date,
        -- Ranking
        ROW_NUMBER() OVER (ORDER BY total_plays DESC) as rank_by_plays,
        ROW_NUMBER() OVER (ORDER BY unique_listeners DESC) as rank_by_listeners,
        -- Popularity score (weighted)
        (total_plays * 0.4 + unique_listeners * 0.4 + unique_sessions * 0.2) as popularity_score
        
    FROM {{ ref('int_song_stats') }}
)

SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY popularity_score DESC) as overall_rank,
    CASE
        WHEN rank_by_plays <= 10 THEN 'Top 10'
        WHEN rank_by_plays <= 50 THEN 'Top 50'
        WHEN rank_by_plays <= 100 THEN 'Top 100'
        ELSE 'Other'
    END as popularity_tier

FROM ranked_songs
ORDER BY popularity_score DESC
