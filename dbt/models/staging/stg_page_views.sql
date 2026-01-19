{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Staging model for page view events
    - Cleans and standardizes raw page view data
    - Maps camelCase (BigQuery external table) to snake_case
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'page_view_events') }}
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
        
        -- Page info
        page,
        
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
        
        -- Location
        {% if target.type == 'bigquery' %}
        location,
        {% else %}
        city,
        state,
        {% endif %}
        
        -- Device
        {% if target.type == 'bigquery' %}
        userAgent AS user_agent,
        {% else %}
        user_agent,
        {% endif %}
        
        -- Derived time fields
        DATE(event_timestamp) AS event_date,
        EXTRACT(HOUR FROM event_timestamp) AS event_hour

    FROM source
    WHERE {% if target.type == 'bigquery' %}userId{% else %}user_id{% endif %} IS NOT NULL
)

SELECT * FROM cleaned
