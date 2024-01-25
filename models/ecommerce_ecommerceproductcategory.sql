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
        "{{ var("table_prefix") }}_products".id ||
        "{{ var("table_prefix") }}_products_categories".id ||
        'productcategory' ||
        'woocommerce'
    )  as id,
    'woocommerce' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    NULL::jsonb as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_products".id as external_id,
    md5(
        '{{ var("integration_id") }}' ||
        "{{ var("table_prefix") }}_products".id ||
        'product' ||
        'woocommerce'
    ) as product_id,
    md5(
        '{{ var("integration_id") }}' ||
        "{{ var("table_prefix") }}_products_categories".id||
        'category' ||
        'woocomerce'
    ) as category_id
FROM "{{ var("table_prefix") }}_products_categories"
LEFT JOIN "{{ var("table_prefix") }}_products"
ON "{{ var("table_prefix") }}_products"._airbyte_{{ var("table_prefix") }}_products_hashid = "{{ var("table_prefix") }}_products_categories"._airbyte_{{ var("table_prefix") }}_products_hashid
