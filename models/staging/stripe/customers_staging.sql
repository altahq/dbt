{{ config(materialized='table', dist='id') }}

SELECT
    metadata,
    subscriptions,
    cards,
    account_balance,
    sources,
    is_deleted,
    balance,
    id,
    email,
    created,
    name,
    updated,
    object
FROM {{source ('stripe', 'customers')}}