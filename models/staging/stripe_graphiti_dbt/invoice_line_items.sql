{{ config(
    enabled=true, 
    materialized='table', 
    dist='id', 
    schema='stripe_graphiti_dbt'
    ) }}

WITH 
base AS (
SELECT
    _airbyte_data,
    _airbyte_data ->> 'id' as id,
    _airbyte_data -> 'period' as period,
    _airbyte_data -> 'plan' as plan,
    _airbyte_data -> 'discount_amounts' as discount_amounts,
    _airbyte_data -> 'tax_amounts' as tax_amounts,
    _airbyte_data ->> 'amount' as amount,
    _airbyte_data ->> 'quantity' as quantity,
    _airbyte_data ->> 'description' as description,
    _airbyte_data ->> 'type' as type,
    _airbyte_data ->> 'invoice_id' as invoice_id
FROM {{source ('stripe_graphiti_dbt', '_airbyte_raw_invoice_line_items')}}
),

{{ dedup_logic('invoice_line_items', var('workspace_id')) }}
