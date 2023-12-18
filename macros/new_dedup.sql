-- deduplication_macro.sql

{% macro new_dedup(primary_key, cursor_field) %}

SELECT
    DISTINCT ON {{ primary_key }}, *
    now() AS dbt_sync_time
FROM base
ORDER BY {{ primary_key }}{{ ',' ~ cursor_field }}
{% endmacro %}