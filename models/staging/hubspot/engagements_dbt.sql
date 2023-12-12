{{ config(enabled=true, materialized='table', dist='id', schema='hubspot_2639e866_3885_4a14_a0ac_53d947f2f907') }}

SELECT
    _airbyte_data ->> 'id' as id,
    _airbyte_data ->> 'createdAt' as createdat,
    _airbyte_data ->> 'lastUpdated' as lastupdated,
    _airbyte_data ->> 'createdBy' as createdby,
    _airbyte_data ->> 'timestamp' as timestamp,
    _airbyte_data ->> 'ownerId' as ownerid,
    _airbyte_data ->> 'teamid' as teamid,
    _airbyte_data ->> 'source' as source,
    _airbyte_data -> 'attachments' as attachments,
    _airbyte_data ->> 'type' as type
FROM {{source ('hubspot', '_airbyte_raw_engagements')}}