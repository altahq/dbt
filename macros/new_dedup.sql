-- deduplication_macro.sql

{% macro new_dedup(primary_key, cursor_field) %}

dedup_cte AS (
  SELECT
    base.*,
    ROW_NUMBER() OVER (PARTITION BY {{ primary_key }} ORDER BY {{ cursor_field }} DESC) AS row_num
  FROM base
)

SELECT
  *,
  now() AS dbt_sync_time
FROM dedup_cte
WHERE row_num = 1
{% endmacro %}