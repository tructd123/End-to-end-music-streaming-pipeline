{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Staging model for listen events
    - Cleans and standardizes raw listen event data
    - Adds derived time fields for analysis
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'listen_events') }}
),

cleaned AS (
    SELECT
        -- Primary identifiers
        event_timestamp,
        user_id,
        session_id,
        
        -- Song info
        song,
        artist,
        
        -- User info
        first_name,
        last_name,
        CONCAT(first_name, ' ', last_name) AS full_name,
        level AS subscription_level,
        
        -- Location
        city,
        state,
        CONCAT(city, ', ', state) AS location,
        
        -- Device
        user_agent,
        
        -- Derived time fields
        DATE(event_timestamp) AS event_date,
        EXTRACT(YEAR FROM event_timestamp) AS event_year,
        EXTRACT(MONTH FROM event_timestamp) AS event_month,
        EXTRACT(DAY FROM event_timestamp) AS event_day,
        EXTRACT(HOUR FROM event_timestamp) AS event_hour,
        {{ day_of_week('event_timestamp') }} AS day_of_week,  -- 0=Sunday, 6=Saturday
        
        -- Time of day category
        CASE 
            WHEN EXTRACT(HOUR FROM event_timestamp) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM event_timestamp) BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN EXTRACT(HOUR FROM event_timestamp) BETWEEN 18 AND 21 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day,
        
        -- Weekend flag
        CASE 
            WHEN {{ day_of_week('event_timestamp') }} IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend

    FROM source
    WHERE 
        user_id IS NOT NULL
        AND song IS NOT NULL
        AND artist IS NOT NULL
)

SELECT * FROM cleaned
