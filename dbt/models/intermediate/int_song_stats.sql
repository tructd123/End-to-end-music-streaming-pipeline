{{
    config(
        materialized='incremental',
        unique_key='song_id',
        partition_by={
            "field": "last_played_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

-- Intermediate model: Song statistics
-- Aggregates listen data by song

WITH song_stats AS (
    SELECT
        -- Create unique song identifier
        FARM_FINGERPRINT(CONCAT(COALESCE(song, ''), '|', COALESCE(artist, ''))) as song_id,
        song,
        artist,
        COUNT(*) as total_plays,
        COUNT(DISTINCT user_id) as unique_listeners,
        COUNT(DISTINCT session_id) as unique_sessions,
        AVG(duration_seconds) as avg_duration_seconds,
        SUM(duration_seconds) as total_listen_time_seconds,
        MIN(event_timestamp) as first_played_at,
        MAX(event_timestamp) as last_played_at,
        MAX(event_date) as last_played_date

    FROM {{ ref('stg_listens') }}
    
    {% if is_incremental() %}
    WHERE event_timestamp > (SELECT MAX(last_played_at) FROM {{ this }})
    {% endif %}
    
    GROUP BY 1, 2, 3
)

SELECT 
    *,
    ROUND(total_listen_time_seconds / 3600, 2) as total_listen_time_hours

FROM song_stats
