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
      "{{ var("table_prefix") }}_orders".id ||
      'customer' ||
      'woocommerce'
    )  as id,
    'woocommerce' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_orders._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_orders".id as external_id,
    "{{ var("table_prefix") }}_orders".number as number,
    "{{ var("table_prefix") }}_orders".created_via as created_via,
    "{{ var("table_prefix") }}_orders".version as version,
    "{{ var("table_prefix") }}_orders".status as status,
    "{{ var("table_prefix") }}_orders".currency as currency,
    "{{ var("table_prefix") }}_orders".discount_total as discount_total,
    "{{ var("table_prefix") }}_orders".discount_tax as discount_tax,
    "{{ var("table_prefix") }}_orders".shipping_total as shipping_total,
    "{{ var("table_prefix") }}_orders".shipping_tax as shipping_tax,
    "{{ var("table_prefix") }}_orders".cart_tax as cart_tax,
    "{{ var("table_prefix") }}_orders".total as total,
    "{{ var("table_prefix") }}_orders".total_tax as total_tax,
    "{{ var("table_prefix") }}_orders".prices_include_tax::boolean as prices_include_tax,
    "{{ var("table_prefix") }}_orders".customer_note as customer_note,
    "{{ var("table_prefix") }}_orders".billing::jsonb as billing,
    "{{ var("table_prefix") }}_orders".shipping::jsonb as shipping,
    "{{ var("table_prefix") }}_orders".payment_method as payment_method,
    "{{ var("table_prefix") }}_orders".transaction_id as transaction_id,
    "{{ var("table_prefix") }}_orders".date_paid::date as date_paid,
    "{{ var("table_prefix") }}_orders".date_completed::date as date_completed,
    "{{ var("table_prefix") }}_orders".line_items::jsonb as lines,
    "{{ var("table_prefix") }}_orders".tax_lines::jsonb as tax_lines,
    "{{ var("table_prefix") }}_orders".shipping_lines::jsonb as shipping_lines,
    "{{ var("table_prefix") }}_orders".fee_lines::jsonb as fee_lines,
    "{{ var("table_prefix") }}_orders".coupon_lines::jsonb as coupon_lines,
    "{{ var("table_prefix") }}_orders".refunds::jsonb as refunds,
    "{{ var("table_prefix") }}_orders".set_paid::boolean as paid,
    "{{ var("table_prefix") }}_orders".date_modified as service_date_updated,
    "{{ var("table_prefix") }}_orders".date_created as service_date_created,
    NULL as invoice_number,
    NULL::date as invoice_date,
    NULL as delivery_number,
    NULL::date as delivery_date,
    NULL as valid,
    NULL as shipping_number,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_orders".customer_id ||
      'customer' ||
      'woocommerce'
    ) as customer_id
FROM "{{ var("table_prefix") }}_orders"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_orders
ON _airbyte_raw_{{ var("table_prefix") }}_orders._airbyte_ab_id = "{{ var("table_prefix") }}_orders"._airbyte_ab_id