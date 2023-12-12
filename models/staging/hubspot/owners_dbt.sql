{{ config(enabled=true, materialized='table', dist='id', schema='hubspot_2639e866_3885_4a14_a0ac_53d947f2f907') }}

SELECT
    _airbyte_data ->> 'id' as id,
    _airbyte_data ->> 'archived' as archived,
    _airbyte_data ->> 'createdAt' as createdat,
    _airbyte_data ->> 'updatedAt' as updatedat,
    _airbyte_data -> 'teams' as teams,
    _airbyte_data ->> 'firstName' as firstname,
    _airbyte_data ->> 'lastName' as lastname,
    _airbyte_data ->> 'email' as email
FROM {{source ('hubspot', '_airbyte_raw_owners')}}