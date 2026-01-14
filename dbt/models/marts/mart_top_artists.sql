{{
    config(
        materialized='table',
        schema='marts'
    )
}}

/*
    Mart: Top Artists
    - Artist performance metrics
*/

WITH song_stats AS (
    SELECT * FROM {{ ref('int_song_stats') }}
),

artist_stats AS (
    SELECT
        artist,
        
        -- Song portfolio
        COUNT(DISTINCT song) AS total_songs,
        
        -- Play metrics (sum across all songs by artist)
        SUM(total_plays) AS total_plays,
        SUM(unique_listeners) AS total_listeners,
        SUM(paid_plays) AS paid_plays,
        SUM(free_plays) AS free_plays,
        
        -- Time range
        MIN(first_played_at) AS first_played_at,
        MAX(last_played_at) AS last_played_at,
        SUM(days_with_plays) AS total_play_days

    FROM song_stats
    GROUP BY artist
)

SELECT
    ROW_NUMBER() OVER (ORDER BY total_plays DESC) AS rank,
    artist,
    total_songs,
    total_plays,
    total_listeners,
    paid_plays,
    free_plays,
    
    -- Derived metrics
    ROUND({{ to_numeric('total_plays') }} / NULLIF(total_songs, 0), 2) AS avg_plays_per_song,
    ROUND({{ to_numeric('total_plays') }} / NULLIF(total_listeners, 0), 2) AS plays_per_listener,
    ROUND({{ to_numeric('paid_plays') }} / NULLIF(total_plays, 0) * 100, 2) AS paid_ratio_pct,
    
    first_played_at,
    last_played_at,
    
    {{ now() }} AS updated_at

FROM artist_stats
ORDER BY total_plays DESC
LIMIT 100
