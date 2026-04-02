{{
    config(
        materialized='incremental',
        schema='marts',
        unique_key=['user_id', 'playlist_name']
    )
}}

/*
    Mart: User Playlists
    - Stores user-created playlists with songs
    - Uses incremental materialization to preserve user-created data
    - Data is written via DML from chatbot tools
    - Compatible with both PostgreSQL (local) and BigQuery (prod)
*/

{% if is_incremental() %}

-- On incremental runs, preserve existing user data
SELECT
    user_id,
    playlist_name,
    songs,
    created_at,
    updated_at
FROM {{ this }}
WHERE 1 = 1

{% else %}

-- Initial run: create empty table structure
{% if target.type == 'bigquery' %}
SELECT
    CAST(NULL AS STRING) AS user_id,
    CAST(NULL AS STRING) AS playlist_name,
    CAST([] AS ARRAY<STRING>) AS songs,
    CAST(NULL AS TIMESTAMP) AS created_at,
    CAST(NULL AS TIMESTAMP) AS updated_at
FROM UNNEST([]) AS empty
{% else %}
SELECT
    CAST(NULL AS VARCHAR) AS user_id,
    CAST(NULL AS VARCHAR) AS playlist_name,
    CAST(NULL AS TEXT) AS songs,
    CAST(NULL AS TIMESTAMP) AS created_at,
    CAST(NULL AS TIMESTAMP) AS updated_at
WHERE FALSE
{% endif %}

{% endif %}
