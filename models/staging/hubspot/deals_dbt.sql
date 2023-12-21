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
    _airbyte_data ->> 'id' as id,
    _airbyte_data -> 'companies' as companies,
    _airbyte_data -> 'contacts' as contacts,
    (_airbyte_data ->> 'archived')::boolean as archived,
    (_airbyte_data ->> 'createdAt')::timestamp as createdat,
    (_airbyte_data ->> 'updatedAt')::timestamp as updatedat,
    _airbyte_data -> 'properties' as properties,
    (_airbyte_data -> 'properties' ->> 'hs_is_closed_won')::boolean as hs_is_closed_won,
    _airbyte_data -> 'properties' ->> 'dealname' as dealname,
    _airbyte_data -> 'properties' ->> 'dealtype' as dealtype,
    (_airbyte_data -> 'properties' ->> 'amount')::float as amount,
    _airbyte_data -> 'properties' ->> 'hubspot_owner_id' as hubspot_owner_id,
    _airbyte_data -> 'properties' ->> 'hs_next_step' as hs_next_step,
    (_airbyte_data -> 'properties' ->> 'closedate')::timestamp as closedate,
    _airbyte_data -> 'properties' ->> 'dealstage' as dealstage,
    _airbyte_data -> 'properties' ->> 'hs_campaign' as hs_campaign,
    _airbyte_data -> 'properties' ->> 'hs_analytics_source' as hs_analytics_source,
    _airbyte_data -> 'properties' ->> 'description' as description,
    (_airbyte_data -> 'properties' ->> 'hs_is_closed')::boolean as hs_is_closed,
    _airbyte_data -> 'properties' ->> 'closed_lost_reason' as closed_lost_reason,
    (_airbyte_data -> 'properties' ->> 'hs_deal_stage_probability')::float as hs_deal_stage_probability,
    (_airbyte_data -> 'properties' ->> 'hs_latest_meeting_activity')::timestamp as hs_latest_meeting_activity,
    (_airbyte_data -> 'properties' ->> 'hs_lastmodifieddate')::timestamp as hs_lastmodifieddate,
    _airbyte_data -> 'properties' ->> 'hs_forecast_amount' as hs_forecast_amount,
    _airbyte_data -> 'properties' ->> 'notes_last_contacted' as notes_last_contacted
FROM {{source ('hubspot', '_airbyte_raw_deals')}}
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