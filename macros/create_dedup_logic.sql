-- deduplication_macro.sql

{% macro dedup_logic(model_name, workspace_id) %}

{% if execute %}

{% set dbt_config_query %}
    SELECT primary_key, cursor_field, sync_mode
    FROM public.dbt_model_configs 
    WHERE 
    airbyte_workspace_id = '{{workspace_id}}' 
    AND model_name  = '{{model_name}}'
{% endset %}



{% set primary_key=run_query(dbt_config_query).columns[0].values()[0] %}
{% set cursor_field=run_query(dbt_config_query).columns[1].values()[0] %}
{% set sync_mode=run_query(dbt_config_query).columns[2].values()[0] %}

{% do log('Sync Mode: ' ~  sync_mode , info=True) %}
{% do log('Model Name: ' ~  model_name , info=True) %}
{% do log('workspace_id: ' ~  workspace_id , info=True) %}
{% do log('Primary Key: ' ~ primary_key, info=True) %}
{% do log('Cursor Field: ' ~ cursor_field, info=True) %}

{% if sync_mode=='incremental_append_dedup' %}

dedup_cte AS (
  SELECT
    base.*,
    ROW_NUMBER() OVER (PARTITION BY {{ primary_key }} ORDER BY {{ cursor_field }} DESC) AS row_num
  FROM base



    {% do log('Dedupping' ~ model_name, info=True) %}
    WHERE {{ cursor_field }} > (SELECT MAX({{ cursor_field }}) FROM base)



)

SELECT
  *
FROM dedup_cte
WHERE row_num = 1

{% elif sync_mode =='full_refresh_overwrite' %}

    {% do log('Full Refresh' ~ model_name, info=True) %}

SELECT
  *
FROM base

{% endif %}


{% endif %}

{% endmacro %}
