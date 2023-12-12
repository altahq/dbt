{{ config(enabled=true, materialized='table', dist='id', schema='hubspot_2639e866_3885_4a14_a0ac_53d947f2f907') }}

SELECT
    _airbyte_data ->> 'id' as id,
    _airbyte_data ->> 'archived' as archived,
    _airbyte_data ->> 'createdAt' as createdat,
    _airbyte_data ->> 'updatedAt' as updatedat,
    _airbyte_data -> 'properties' as properties,
    _airbyte_data -> 'properties' ->> 'name' as name,
    _airbyte_data -> 'properties' ->> 'phone' as phone,
    _airbyte_data -> 'properties' ->> 'type' as type,
    _airbyte_data -> 'properties' ->> 'industry' as industry,
    _airbyte_data -> 'properties' ->> 'website' as website,
    _airbyte_data -> 'properties' ->> 'description' as description,
    _airbyte_data -> 'properties' ->> 'hubspot_owner_id' as hubspot_owner_id,
    _airbyte_data -> 'properties' ->> 'hs_created_by_user_id' as hs_created_by_user_id,
    _airbyte_data -> 'properties' ->> 'hs_analytics_source' as hs_analytics_source,
    _airbyte_data -> 'properties' ->> 'annualrevenue' as annualrevenue,
    _airbyte_data -> 'properties' ->> 'hs_last_sales_activity_date' as hs_last_sales_activity_date,
    _airbyte_data -> 'properties' ->> 'hs_lastmodifieddate' as hs_lastmodifieddate,
    _airbyte_data -> 'properties' ->> 'numemployees' as numemployees
FROM {{source ('hubspot', '_airbyte_raw_companies')}}