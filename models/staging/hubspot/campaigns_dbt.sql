{{ config(enabled=true, materialized='table', dist='id', schema='hubspot_2639e866_3885_4a14_a0ac_53d947f2f907') }}

SELECT
    _airbyte_data ->> 'id' as id,
    _airbyte_data ->> 'type' as type,
    _airbyte_data ->> 'name' as name,
    _airbyte_data ->> 'appName' as appname,
    _airbyte_data ->> 'subject' as subject,
    _airbyte_data ->> 'lastUpdatedTime' as lastupdatedtime
FROM {{source ('hubspot', '_airbyte_raw_contacts')}}