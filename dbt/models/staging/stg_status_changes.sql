{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Staging model for status change events
    - User subscription level changes (free -> paid, paid -> free)
    - Maps camelCase (BigQuery external table) to snake_case
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'status_change_events') }}
),

cleaned AS (
    SELECT
        -- Primary identifiers
        event_timestamp,
        {% if target.type == 'bigquery' %}
        userId AS user_id,
        {% else %}
        user_id,
        {% endif %}
        
        -- Status info (BigQuery uses 'level', Postgres uses 'new_level')
        {% if target.type == 'bigquery' %}
        level AS new_level,
        {% else %}
        new_level,
        {% endif %}
        
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
        
        -- Location
        {% if target.type == 'bigquery' %}
        location,
        {% else %}
        city,
        state,
        {% endif %}
        
        -- Derived time fields
        DATE(event_timestamp) AS event_date

    FROM source
    WHERE {% if target.type == 'bigquery' %}userId{% else %}user_id{% endif %} IS NOT NULL
)

SELECT * FROM cleaned
