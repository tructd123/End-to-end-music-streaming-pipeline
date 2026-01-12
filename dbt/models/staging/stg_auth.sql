{{
    config(
        materialized='view'
    )
}}

-- Staging model for auth events
-- Cleans and standardizes authentication event data

SELECT
    ts,
    event_timestamp,
    user_id,
    session_id,
    page,
    auth as auth_status,
    level as subscription_level,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) as full_name,
    gender,
    location,
    user_agent,
    CASE 
        WHEN success = 'true' THEN TRUE 
        ELSE FALSE 
    END as is_success,
    processed_at,
    year,
    month,
    day,
    hour,
    -- Derived fields
    DATE(event_timestamp) as event_date

FROM {{ source('raw', 'stg_auth_events') }}
