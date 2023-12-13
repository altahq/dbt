-- deduplication_macro.sql

{% macro dedup_logic(primary_key, cursor_field, model_name) %}

dedup_cte AS (
  SELECT
    base.*,
    ROW_NUMBER() OVER (PARTITION BY {{ primary_key }} ORDER BY {{ cursor_field }} DESC) AS row_num
  FROM base
)

SELECT
  *
FROM dedup_cte
WHERE row_num = 1
{% endmacro %}
