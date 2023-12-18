{{ config(
    enabled=true, 
    materialized=get_config( 'customers', var('workspace_id'))['sync_mode'], 
    dist=get_config( 'customers', var('workspace_id'))['primary_key'], 
    schema='stripe_graphiti_dbt'
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
)

select * from base