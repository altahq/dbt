{{ config(enabled=true, materialized='table', dist='id', schema='stripe_graphiti_dbt') }}

WITH 
base AS (
SELECT
    _airbyte_data,
    _airbyte_data -> 'metadata' as metadata,
    _airbyte_data ->> 'balance' as balance,
    _airbyte_data ->> 'id' as id,
    _airbyte_data ->> 'email' as email,
    _airbyte_data ->> 'created' as created,
    _airbyte_data ->> 'name' as name,
    _airbyte_data ->> 'updated' as updated,
    _airbyte_data ->> 'object' as object
FROM {{source ('stripe_graphiti_dbt', '_airbyte_raw_customers')}}
),

{% set model_config_query %}
    SELECT 
        primary_key, cursor_field, sync_mode
    FROM public.dbt_model_configs 
    WHERE 
        airbyte_workspace_id = ' {{var('workspace_id')}} '
        AND model_name = ' {{ this.name }} '
{% endset %}

{% set results = run_query(model_config_query) %}
{% if execute %}

{% set primary_key = results.columns[0].values() %}
{% set cursor_field = results.columns[1].values() %}
{% set sync_mode = results.columns[2].values() %}

{% endif %}





{{ dedup_logic( primary_key = {{ primary_key }} , cursor_field =  {{ cursor_field }} ) }}