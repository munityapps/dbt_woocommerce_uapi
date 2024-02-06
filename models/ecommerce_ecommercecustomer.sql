{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT 
    NOW() as created,
    NOW() as modified,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_customers".id ||
      'customer' ||
      'woocommerce'
    )  as id,
    'woocommerce' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_customers._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_customers".id as external_id,
    "{{ var("table_prefix") }}_customers".first_name as firstname,
    "{{ var("table_prefix") }}_customers".last_name as lastname,
    "{{ var("table_prefix") }}_customers".username as username,
    NULL::date as birthday,
    "{{ var("table_prefix") }}_customers".email as email,
    NULL as address,
    NULL::boolean as email_marketing_consent,
    NULL::integer as order_count,
    NULL as state,
    NULL::float as total_spent,
    NULL as note,
    NULL as phone,
    NULL::jsonb as addresses,
    NULL as tags,
    "{{ var("table_prefix") }}_customers".role as role,
    NULL::boolean as tax_exempt,
    "{{ var("table_prefix") }}_customers".billing::jsonb as billing,
    "{{ var("table_prefix") }}_customers".shipping::jsonb as shipping,
    "{{ var("table_prefix") }}_customers".avatar_url as avatar_url,
    TRUE as active,
    NULL::boolean as is_guest,
    "{{ var("table_prefix") }}_customers".date_modified as service_date_updated,
    "{{ var("table_prefix") }}_customers".date_created as service_date_created
FROM "{{ var("table_prefix") }}_customers"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_customers
ON _airbyte_raw_{{ var("table_prefix") }}_customers._airbyte_ab_id = "{{ var("table_prefix") }}_customers"._airbyte_ab_id