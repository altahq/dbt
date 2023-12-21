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
    (_airbyte_data ->> 'archived')::boolean as archived,
    (_airbyte_data ->> 'createdAt')::timestamp as createdat,
    (_airbyte_data ->> 'updatedAt')::timestamp as updatedat,
    _airbyte_data -> 'properties' as properties,
    _airbyte_data -> 'properties' ->> 'email' as email,
    _airbyte_data -> 'properties' ->> 'phone' as phone,
    _airbyte_data -> 'properties' ->> 'jobtitle' as jobtitle,
    _airbyte_data -> 'properties' ->> 'rating' as rating,
    _airbyte_data -> 'properties' ->> 'hs_lead_status' as hs_lead_status,
    _airbyte_data -> 'properties' ->> 'company' as company,
    _airbyte_data -> 'properties' ->> 'country' as country,
    _airbyte_data -> 'properties' ->> 'hubspot_owner_id' as hubspot_owner_id,
    _airbyte_data -> 'properties' ->> 'industry' as industry,
    _airbyte_data -> 'properties' ->> 'hs_latest_source' as hs_latest_source,
    _airbyte_data -> 'properties' ->> 'hs_created_by_user_id' as hs_created_by_user_id,
    _airbyte_data -> 'properties' ->> 'message' as message,
    _airbyte_data -> 'properties' ->> 'first_deal_created_date' as first_deal_created_date,
    _airbyte_data -> 'properties' ->> 'annualrevenue' as annualrevenue,
    (_airbyte_data -> 'properties' ->> 'hs_last_sales_activity_date')::timestamp as hs_last_sales_activity_date,
    _airbyte_data -> 'properties' ->> 'state' as state,
    _airbyte_data -> 'properties' ->> 'numemployees' as numemployees,
    _airbyte_data -> 'properties' ->> 'associatedcompanyid' as associatedcompanyid,
    _airbyte_data -> 'properties' ->> 'full_name' as full_name
FROM {{source ('hubspot', '_airbyte_raw_contacts')}}
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