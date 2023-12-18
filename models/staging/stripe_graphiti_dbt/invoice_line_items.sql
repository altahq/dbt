{% if execute %}

{% set materialize_mode = get_config( this.name, var('workspace_id'))['materialize_mode'] %}
{% set primary_key = get_config( this.name, var('workspace_id'))['primary_key'] %}
{% set cursor_field = get_config( this.name, var('workspace_id'))['cursor_field'] %}
{% set full_refresh = get_config( this.name, var('workspace_id'))['full_refresh'] %}


{{ config(
    enabled=true, 
    materialized=materialize_mode, 
    unique_key=primary_key,
    full_refresh=full_refresh
    ) }}

{% do log('Primary Key: ' ~ primary_key, info=True) %}
{% do log('Materilization Mode: ' ~ materialize_mode, info=True) %}
{% do log('Schema: ' ~ config.get('schema'), info=True) %}

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