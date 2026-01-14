{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Staging model for status change events
    - User subscription level changes (free -> paid, paid -> free)
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'status_change_events') }}
),

cleaned AS (
    SELECT
        -- Primary identifiers
        event_timestamp,
        user_id,
        
        -- Status info
        new_level,
        
        -- User info
        first_name,
        last_name,
        CONCAT(first_name, ' ', last_name) AS full_name,
        
        -- Location
        city,
        state,
        
        -- Derived time fields
        DATE(event_timestamp) AS event_date

    FROM source
    WHERE user_id IS NOT NULL
)

SELECT * FROM cleaned
