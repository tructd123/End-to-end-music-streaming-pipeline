{{
    config(
        materialized='incremental',
        schema='marts',
        unique_key='event_date',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

/*
    Mart: Daily Summary (INCREMENTAL)
    - Executive dashboard KPIs by day
    - Merge strategy: updates today's data, keeps historical
*/

WITH daily_metrics AS (
    SELECT * FROM {{ ref('int_daily_metrics') }}
    {% if is_incremental() %}
    -- Only process last 2 days to handle late-arriving data and recalculate today
    {% if target.type == 'postgres' %}
    WHERE event_date >= (SELECT MAX(event_date) - INTERVAL '1 day' FROM {{ this }})
    {% elif target.type == 'bigquery' %}
    WHERE event_date >= (SELECT DATE_SUB(MAX(event_date), INTERVAL 1 DAY) FROM {{ this }})
    {% endif %}
    {% endif %}
)

SELECT
    event_date,
    
    -- Volume KPIs
    total_plays,
    unique_users,
    unique_songs,
    unique_artists,
    total_sessions,
    
    -- Revenue indicators
    paid_plays,
    free_plays,
    paid_users,
    free_users,
    paid_plays_pct,
    paid_users_pct,
    
    -- Engagement
    plays_per_user,
    plays_per_session,
    
    -- Time distribution
    morning_plays,
    afternoon_plays,
    evening_plays,
    night_plays,
    
    -- Day-over-day change (using window functions)
    total_plays - LAG(total_plays) OVER (ORDER BY event_date) AS plays_change,
    unique_users - LAG(unique_users) OVER (ORDER BY event_date) AS users_change,
    
    -- 7-day rolling averages
    ROUND(AVG(total_plays) OVER (
        ORDER BY event_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS plays_7day_avg,
    
    ROUND(AVG(unique_users) OVER (
        ORDER BY event_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS users_7day_avg,
    
    {{ now() }} AS updated_at

FROM daily_metrics
ORDER BY event_date DESC
