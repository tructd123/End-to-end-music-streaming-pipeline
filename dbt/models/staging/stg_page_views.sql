{{
    config(
        materialized='view'
    )
}}

-- Staging model for page view events
-- Cleans and standardizes raw page view data

SELECT
    ts,
    event_timestamp,
    user_id,
    session_id,
    page,
    method,
    status as http_status,
    level as subscription_level,
    auth as auth_status,
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
    DATE(event_timestamp) as event_date

FROM {{ source('raw', 'stg_page_view_events') }}
WHERE user_id IS NOT NULL
