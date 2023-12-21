{% if execute %}

{% set config_values = get_config(this.name, var('workspace_id')) %}
{% set materialize_mode = config_values['materialize_mode'] %}
{% set primary_key = config_values['primary_key'] %}
{% set cursor_field = config_values['cursor_field'] %}
{% set full_refresh = config_values['full_refresh'] %}


{{ config(
    enabled=true, 
    materialized=materialize_mode, 
    unique_key=primary_key,
    full_refresh=full_refresh
    ) }}

{% do log(this.name ~ ' from within model - Primary Key: ' ~ primary_key, info=True) %}
{% do log(this.name ~ ' from within model - Materilization Mode: ' ~ materialize_mode, info=True) %}

WITH 
base AS (
SELECT
    _airbyte_data,
    _airbyte_data ->> 'id' as id,
    (_airbyte_data ->> 'archived')::boolean as archived,
    (_airbyte_data ->> 'createdAt')::timestamp as createdat,
    (_airbyte_data ->> 'updatedAt')::timestamp as updatedat,
    _airbyte_data -> 'teams' as teams,
    _airbyte_data ->> 'firstName' as firstname,
    _airbyte_data ->> 'lastName' as lastname,
    _airbyte_data ->> 'email' as email
FROM {{source ('hubspot', '_airbyte_raw_owners')}}
)

{% if materialize_mode == 'incremental' %}

    {{ dedup_logic(primary_key, cursor_field) }}

{% else %}

SELECT *,
    now() AS dbt_sync_time
FROM
base

{% endif %}

{% endif %}