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
    _airbyte_data ->> 'type' as type,
    _airbyte_data ->> 'name' as name,
    _airbyte_data ->> 'appName' as appname,
    _airbyte_data ->> 'subject' as subject,
    (_airbyte_data ->> 'lastUpdatedTime')::bigint as lastupdatedtime
FROM {{source ('hubspot', '_airbyte_raw_campaigns')}}
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