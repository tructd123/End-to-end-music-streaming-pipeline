/*
    Cross-database compatibility macros
    Supports: PostgreSQL (local) and BigQuery (prod)
*/

-- Day of week extraction (0=Sunday for consistency)
{% macro day_of_week(timestamp_column) %}
    {% if target.type == 'postgres' %}
        EXTRACT(DOW FROM {{ timestamp_column }})
    {% elif target.type == 'bigquery' %}
        EXTRACT(DAYOFWEEK FROM {{ timestamp_column }}) - 1
    {% endif %}
{% endmacro %}

-- Cast to numeric
{% macro to_numeric(column_expr) %}
    {% if target.type == 'postgres' %}
        {{ column_expr }}::NUMERIC
    {% elif target.type == 'bigquery' %}
        CAST({{ column_expr }} AS NUMERIC)
    {% endif %}
{% endmacro %}

-- Create timestamp from date and hour
{% macro date_add_hours(date_column, hours_column) %}
    {% if target.type == 'postgres' %}
        {{ date_column }} + ({{ hours_column }} || ' hours')::INTERVAL
    {% elif target.type == 'bigquery' %}
        TIMESTAMP_ADD(TIMESTAMP({{ date_column }}), INTERVAL {{ hours_column }} HOUR)
    {% endif %}
{% endmacro %}

-- Subtract interval from timestamp
{% macro timestamp_sub_hours(timestamp_expr, hours) %}
    {% if target.type == 'postgres' %}
        {{ timestamp_expr }} - INTERVAL '{{ hours }} hours'
    {% elif target.type == 'bigquery' %}
        TIMESTAMP_SUB({{ timestamp_expr }}, INTERVAL {{ hours }} HOUR)
    {% endif %}
{% endmacro %}

-- Default timestamp for incremental
{% macro default_timestamp() %}
    {% if target.type == 'postgres' %}
        '1970-01-01'::timestamp
    {% elif target.type == 'bigquery' %}
        TIMESTAMP('1970-01-01')
    {% endif %}
{% endmacro %}

-- Date truncation
{% macro trunc_date(timestamp_column, granularity) %}
    {% if target.type == 'postgres' %}
        DATE_TRUNC('{{ granularity }}', {{ timestamp_column }})
    {% elif target.type == 'bigquery' %}
        {% if granularity == 'day' %}
            DATE({{ timestamp_column }})
        {% elif granularity == 'week' %}
            DATE_TRUNC(DATE({{ timestamp_column }}), WEEK)
        {% elif granularity == 'month' %}
            DATE_TRUNC(DATE({{ timestamp_column }}), MONTH)
        {% endif %}
    {% endif %}
{% endmacro %}

-- Current timestamp
{% macro now() %}
    {% if target.type == 'postgres' %}
        CURRENT_TIMESTAMP
    {% elif target.type == 'bigquery' %}
        CURRENT_TIMESTAMP()
    {% endif %}
{% endmacro %}
