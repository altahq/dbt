{{ config(enabled=true, materialized='table', dist='id', schema='hubspot_2639e866_3885_4a14_a0ac_53d947f2f907') }}

SELECT
    _airbyte_data ->> 'id' as id,
    _airbyte_data ->> 'archived' as archived,
    _airbyte_data ->> 'createdAt' as createdat,
    _airbyte_data ->> 'updatedAt' as updatedat,
    _airbyte_data -> 'properties' as properties,
    _airbyte_data -> 'properties' ->> 'hs_is_closed_won' as hs_is_closed_won,
    _airbyte_data -> 'properties' ->> 'dealname' as dealname,
    _airbyte_data -> 'properties' ->> 'dealtype' as dealtype,
    _airbyte_data -> 'properties' ->> 'amount' as amount,
    _airbyte_data -> 'properties' ->> 'hubspot_owner_id' as hubspot_owner_id,
    _airbyte_data -> 'properties' ->> 'hs_next_step' as hs_next_step,
    _airbyte_data -> 'properties' ->> 'closedate' as closedate,
    _airbyte_data -> 'properties' ->> 'dealstage' as dealstage,
    _airbyte_data -> 'properties' ->> 'hs_campaign' as hs_campaign,
    _airbyte_data -> 'properties' ->> 'hs_analytics_source' as hs_analytics_source,
    _airbyte_data -> 'properties' ->> 'description' as description,
    _airbyte_data -> 'properties' ->> 'hs_is_closed' as hs_is_closed,
    _airbyte_data -> 'properties' ->> 'closed_lost_reason' as closed_lost_reason,
    _airbyte_data -> 'properties' ->> 'hs_deal_stage_probability' as hs_deal_stage_probability,
    _airbyte_data -> 'properties' ->> 'hs_latest_meeting_activity' as hs_latest_meeting_activity,
    _airbyte_data -> 'properties' ->> 'hs_lastmodifieddate' as hs_lastmodifieddate,
    _airbyte_data -> 'properties' ->> 'hs_forecast_amount' as hs_forecast_amount,
    _airbyte_data -> 'properties' ->> 'notes_last_contacted' as notes_last_contacted
FROM {{source ('hubspot', '_airbyte_raw_deals')}}