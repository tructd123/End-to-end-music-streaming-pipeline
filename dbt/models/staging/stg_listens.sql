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
    - Maps camelCase (BigQuery external table) to snake_case
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'listen_events') }}
),

cleaned AS (
    SELECT
        -- Primary identifiers
        event_timestamp,
        {% if target.type == 'bigquery' %}
        userId AS user_id,
        sessionId AS session_id,
        {% else %}
        user_id,
        session_id,
        {% endif %}
        
        -- Song info
        song,
        artist,
        
        -- User info
        {% if target.type == 'bigquery' %}
        firstName AS first_name,
        lastName AS last_name,
        CONCAT(firstName, ' ', lastName) AS full_name,
        {% else %}
        first_name,
        last_name,
        CONCAT(first_name, ' ', last_name) AS full_name,
        {% endif %}
        level AS subscription_level,
        
        -- Location (BigQuery external table has location as full string)
        {% if target.type == 'bigquery' %}
        location,
        {% else %}
        city,
        state,
        CONCAT(city, ', ', state) AS location,
        {% endif %}
        
        -- Device
        {% if target.type == 'bigquery' %}
        userAgent AS user_agent,
        {% else %}
        user_agent,
        {% endif %}
        
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
        {% if target.type == 'bigquery' %}
        userId IS NOT NULL
        {% else %}
        user_id IS NOT NULL
        {% endif %}
        AND song IS NOT NULL
        AND artist IS NOT NULL
)

SELECT * FROM cleaned
