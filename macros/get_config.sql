-- get_config_macro.sql

{% macro get_config(model_name, workspace_id) %}

{% if execute %}

{% set dbt_config_query %}
    SELECT primary_key, cursor_field, sync_mode
    FROM public.dbt_model_configs 
    WHERE 
    airbyte_workspace_id = '{{workspace_id}}' 
    AND model_name  = '{{model_name}}'
{% endset %}

{% set table_exists_query %}
    SELECT NOT EXISTS
    (
        SELECT 
            1 
        FROM 
        information_schema.tables 
        WHERE 
        table_schema = '{{ this.schema }}'
        AND table_name = '{{ model_name }}'
    )
{% endset %}

{% do log('Schema: ' ~  this.schema , info=True) %}


{% set table_exists=run_query(table_exists_query).columns[0].values()[0] %}
{% set primary_key=run_query(dbt_config_query).columns[0].values()[0] %}
{% set cursor_field=run_query(dbt_config_query).columns[1].values()[0] %}
{% set sync_mode=run_query(dbt_config_query).columns[2].values()[0] %}

{% do log('Sync Mode: ' ~  sync_mode , info=True) %}
{% do log('Model Name: ' ~  model_name , info=True) %}
{% do log('workspace_id: ' ~  workspace_id , info=True) %}
{% do log('Primary Key: ' ~ primary_key, info=True) %}
{% do log('Cursor Field: ' ~ cursor_field, info=True) %}

{% set materialize_mode='table' %}

{% if sync_mode == 'incremental_append_dedup' %}

{% set materialize_mode='incremental' %}

{% endif %}

{{ return ({'primary_key': primary_key, 'cursor_field': cursor_field, 'materialize_mode': materialize_mode, 'full_refresh': table_exists}) }}

{% endif %}

{% endmacro %}
