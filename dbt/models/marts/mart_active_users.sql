{{
    config(
        materialized='table',
        schema='marts'
    )
}}

/*
    Mart: Active Users
    - User engagement and activity summary
    - Materialized as table for fast queries
*/

WITH user_activity AS (
    SELECT * FROM {{ ref('int_user_activity') }}
)

SELECT
    user_id,
    full_name,
    current_level,
    {% if target.type == 'bigquery' %}
    location,
    {% else %}
    city,
    state,
    {% endif %}
    
    -- Activity metrics
    total_plays,
    unique_songs,
    unique_artists,
    total_sessions,
    active_days,
    
    -- Engagement
    avg_plays_per_active_day,
    avg_plays_per_session,
    song_diversity_pct,
    engagement_tier,
    
    -- Preferences
    favorite_time,
    preferred_days,
    
    -- Time breakdown
    morning_plays,
    afternoon_plays,
    evening_plays,
    night_plays,
    weekend_plays,
    weekday_plays,
    
    -- Timeline
    first_listen_at,
    last_listen_at,
    listening_span_days,
    
    -- Recency score (days since last listen)
    {% if target.type == 'postgres' %}
    EXTRACT(DAY FROM (CURRENT_TIMESTAMP - last_listen_at)) AS days_since_last_listen,
    {% elif target.type == 'bigquery' %}
    DATE_DIFF(CURRENT_DATE(), DATE(last_listen_at), DAY) AS days_since_last_listen,
    {% endif %}
    
    -- Is user still active (listened in last 7 days)?
    CASE 
        {% if target.type == 'postgres' %}
        WHEN last_listen_at >= CURRENT_DATE - INTERVAL '7 days' THEN TRUE
        {% elif target.type == 'bigquery' %}
        WHEN last_listen_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) THEN TRUE
        {% endif %}
        ELSE FALSE
    END AS is_active,
    
    {{ now() }} AS updated_at

FROM user_activity
ORDER BY total_plays DESC
