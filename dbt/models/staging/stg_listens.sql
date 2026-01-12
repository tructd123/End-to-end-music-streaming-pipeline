{{
    config(
        materialized='view'
    )
}}

-- Staging model for listen events
-- Cleans and standardizes raw listen event data

SELECT
    ts,
    event_timestamp,
    user_id,
    session_id,
    song,
    artist,
    CAST(duration AS FLOAT64) as duration_seconds,
    level as subscription_level,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) as full_name,
    gender,
    location,
    latitude,
    longitude,
    user_agent,
    processed_at,
    year,
    month,
    day,
    hour,
    -- Derived fields
    DATE(event_timestamp) as event_date,
    EXTRACT(DAYOFWEEK FROM event_timestamp) as day_of_week,
    CASE 
        WHEN EXTRACT(HOUR FROM event_timestamp) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM event_timestamp) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM event_timestamp) BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END as time_of_day

FROM {{ source('raw', 'stg_listen_events') }}
WHERE user_id IS NOT NULL
  AND song IS NOT NULL
