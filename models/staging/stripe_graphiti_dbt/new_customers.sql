{% if execute %}

{% set materialize_mode = get_config( 'customers', var('workspace_id'))['materialize_mode'] %}
{% set primary_key = get_config( 'customers', var('workspace_id'))['primary_key'] %}
{% set cursor_field = get_config( 'customers', var('workspace_id'))['cursor_field'] %}
{% set full_refresh = get_config( 'customers', var('workspace_id'))['full_refresh'] %}


{{ config(
    enabled=true, 
    materialized=materialize_mode, 
    unique_key=primary_key,
    full_refresh=full_refresh
    ) }}

{% do log('Primary Key: ' ~ primary_key, info=True) %}
{% do log('Materilization Mode: ' ~ materialize_mode, info=True) %}
{% do log('Schema: ' ~ config.get('schema'), info=True) %}

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

{{ new_dedup(primary_key, cursor_field) }}

{% endif %}