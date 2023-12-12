{{ config(enabled=true, materialized='table', dist='id', schema='hubspot_2639e866_3885_4a14_a0ac_53d947f2f907') }}

SELECT
    _airbyte_data -> 'properties' as properties,
    _airbyte_data ->> 'id' as id,
    _airbyte_data ->> 'archived' as archived,
    _airbyte_data ->> 'createdAt' as createdat,
    _airbyte_data ->> 'updatedAt' as updatedat,
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
    _airbyte_data -> 'properties' ->> 'hs_last_sales_activity_date' as hs_last_sales_activity_date,
    _airbyte_data -> 'properties' ->> 'state' as state,
    _airbyte_data -> 'properties' ->> 'numemployees' as numemployees,
    _airbyte_data -> 'properties' ->> 'associatedcompanyid' as associatedcompanyid,
    _airbyte_data -> 'properties' ->> 'full_name' as full_name
FROM {{source ('hubspot', '_airbyte_raw_contacts')}}