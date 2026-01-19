{{
    config(
        materialized='incremental',
        schema='marts',
        unique_key='location',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

/*
    Mart: Location Analytics (INCREMENTAL)
    - Geographic distribution of listening activity
    - Useful for regional marketing and content strategy
    - Merge strategy: updates locations with new activity
*/

WITH listens AS (
    SELECT * FROM {{ ref('stg_listens') }}
    {% if is_incremental() %}
    -- Only process data from last 6 hours for location updates
    WHERE event_timestamp > (
        SELECT {{ timestamp_sub_hours('COALESCE(MAX(last_activity), ' ~ default_timestamp() ~ ')', 6) }}
        FROM {{ this }}
    )
    {% endif %}
),

location_stats AS (
    SELECT
        {% if target.type == 'bigquery' %}
        location,
        {% else %}
        city,
        state,
        location,
        {% endif %}
        
        -- Volume
        COUNT(*) AS total_plays,
        COUNT(DISTINCT user_id) AS unique_users,
        COUNT(DISTINCT song) AS unique_songs,
        COUNT(DISTINCT artist) AS unique_artists,
        
        -- User breakdown
        COUNT(DISTINCT CASE WHEN subscription_level = 'paid' THEN user_id END) AS paid_users,
        COUNT(DISTINCT CASE WHEN subscription_level = 'free' THEN user_id END) AS free_users,
        
        -- Time analysis
        MIN(event_timestamp) AS first_activity,
        MAX(event_timestamp) AS last_activity

    FROM listens
    WHERE location IS NOT NULL
    {% if target.type == 'bigquery' %}
    GROUP BY location
    {% else %}
    GROUP BY city, state, location
    {% endif %}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY total_plays DESC) AS rank,
    {% if target.type == 'postgres' %}
    city,
    state,
    {% endif %}
    location,
    total_plays,
    unique_users,
    unique_songs,
    unique_artists,
    paid_users,
    free_users,
    
    -- Derived metrics
    ROUND({{ to_numeric('total_plays') }} / NULLIF(unique_users, 0), 2) AS plays_per_user,
    ROUND({{ to_numeric('paid_users') }} / NULLIF(unique_users, 0) * 100, 2) AS paid_user_pct,
    
    first_activity,
    last_activity,
    
    {{ now() }} AS updated_at

FROM location_stats
ORDER BY total_plays DESC
