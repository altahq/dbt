-- deduplication_macro.sql

{% macro new_dedup(primary_key, cursor_field) %}

SELECT
    DISTINCT ON {{ primary_key }}, *
    now() AS dbt_sync_time
FROM base
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  WHERE {{ cursor_field }} > (SELECT MAX({{ cursor_field }}) FROM base)

{% endif %}
ORDER BY {{ primary_key }}{{ ',' ~ cursor_field }}
{% endmacro %}