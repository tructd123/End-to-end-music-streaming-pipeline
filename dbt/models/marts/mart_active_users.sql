{{
    config(
        materialized='table',
        partition_by={
            "field": "last_active_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

-- MART: Active Users Dashboard
-- Answers: "Người dùng nào đang active? Họ đến từ đâu?"

WITH user_segments AS (
    SELECT
        user_id,
        first_name,
        last_name,
        full_name,
        gender,
        location,
        subscription_level,
        total_listens,
        unique_songs,
        unique_artists,
        total_sessions,
        total_listen_hours,
        total_page_views,
        avg_listens_per_session,
        first_listen_at,
        last_listen_at,
        last_active_date,
        hours_since_last_activity,
        
        -- User segmentation by activity
        CASE
            WHEN hours_since_last_activity <= {{ var('active_user_threshold_hours') }} THEN 'Active Now'
            WHEN hours_since_last_activity <= 168 THEN 'Active This Week'
            WHEN hours_since_last_activity <= 720 THEN 'Active This Month'
            ELSE 'Inactive'
        END as activity_status,
        
        -- User segmentation by engagement
        CASE
            WHEN total_listen_hours >= 50 AND unique_artists >= 20 THEN 'Super Fan'
            WHEN total_listen_hours >= 20 AND unique_artists >= 10 THEN 'Regular Listener'
            WHEN total_listen_hours >= 5 THEN 'Casual Listener'
            ELSE 'New User'
        END as engagement_tier,
        
        -- Extract location components
        SPLIT(location, ',')[SAFE_OFFSET(0)] as city,
        TRIM(SPLIT(location, ',')[SAFE_OFFSET(1)]) as state
        
    FROM {{ ref('int_user_activity') }}
)

SELECT
    *,
    -- Ranking
    ROW_NUMBER() OVER (ORDER BY total_listen_hours DESC) as rank_by_listen_time,
    ROW_NUMBER() OVER (ORDER BY total_listens DESC) as rank_by_plays,
    
    -- Activity score
    (total_listens * 0.3 + total_listen_hours * 0.3 + unique_artists * 0.2 + total_sessions * 0.2) as engagement_score

FROM user_segments
ORDER BY engagement_score DESC
