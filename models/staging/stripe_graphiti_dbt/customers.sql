{{ config(
    enabled=true, 
    materialized='table', 
    dist='id', 
    schema='stripe_graphiti_dbt',
    primary_key=run_query("SELECT primary_key FROM public.dbt_model_configs WHERE airbyte_workspace_id = ' {{var('workspace_id')}} ' AND model_name = ' {{ this.name }} '").columns[0].values(),
    cursor_field=run_query("SELECT cursor_field FROM public.dbt_model_configs WHERE airbyte_workspace_id = ' {{var('workspace_id')}} ' AND model_name = ' {{ this.name }} '").columns[0].values()
    ) }}

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


{{ dedup_logic( config(primary_key)  ,  config(cursor_field)  , this.name) }}