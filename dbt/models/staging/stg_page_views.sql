{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Staging model for page view events
    - Cleans and standardizes raw page view data
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'page_view_events') }}
),

cleaned AS (
    SELECT
        -- Primary identifiers
        event_timestamp,
        user_id,
        session_id,
        
        -- Page info
        page,
        
        -- User info
        first_name,
        last_name,
        CONCAT(first_name, ' ', last_name) AS full_name,
        level AS subscription_level,
        
        -- Location
        city,
        state,
        
        -- Device
        user_agent,
        
        -- Derived time fields
        DATE(event_timestamp) AS event_date,
        EXTRACT(HOUR FROM event_timestamp) AS event_hour

    FROM source
    WHERE user_id IS NOT NULL
)

SELECT * FROM cleaned
