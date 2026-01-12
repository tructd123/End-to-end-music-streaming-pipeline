{{
    config(
        materialized='table'
    )
}}

-- MART: User Location Analytics
-- Answers: "Họ đến từ đâu?"

WITH user_locations AS (
    SELECT
        location,
        SPLIT(location, ',')[SAFE_OFFSET(0)] as city,
        TRIM(SPLIT(location, ',')[SAFE_OFFSET(1)]) as state,
        COUNT(DISTINCT user_id) as total_users,
        SUM(total_listens) as total_listens,
        SUM(total_listen_hours) as total_listen_hours,
        AVG(total_listen_hours) as avg_listen_hours_per_user,
        COUNTIF(activity_status = 'Active Now') as active_users,
        COUNTIF(subscription_level = 'paid') as paid_users,
        COUNTIF(subscription_level = 'free') as free_users
        
    FROM {{ ref('mart_active_users') }}
    GROUP BY 1, 2, 3
)

SELECT
    *,
    ROUND(paid_users * 100.0 / NULLIF(total_users, 0), 2) as paid_user_percentage,
    ROUND(active_users * 100.0 / NULLIF(total_users, 0), 2) as active_user_percentage,
    ROW_NUMBER() OVER (ORDER BY total_users DESC) as rank_by_users,
    ROW_NUMBER() OVER (ORDER BY total_listens DESC) as rank_by_listens

FROM user_locations
ORDER BY total_users DESC
