-- deduplication_macro.sql

{% macro dedup_logic(model_name, workspace_id) %}

{% if execute %}

{% set primary_key=run_query("SELECT primary_key FROM public.dbt_model_configs WHERE airbyte_workspace_id = ' {{ workspace_id }} ' AND {{ model_name}}  = ' model_name '").columns[0].values() %}
{% set cursor_field=run_query("SELECT cursor_field FROM public.dbt_model_configs WHERE airbyte_workspace_id = ' {{ workspace_id }} ' AND {{ model_name}}  = ' model_name '").columns[0].values() %}


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

{% endif %}

{% endmacro %}
