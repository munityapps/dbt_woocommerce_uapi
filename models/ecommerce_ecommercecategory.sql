{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT 
    distinct id as external_id,
    NOW() as created,
    NOW() as modified,
    md5(
      '{{ var("integration_id") }}' ||
      id ||
      'category' ||
      'woocommerce'
    )  as id,
    'woocommerce' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    NULL::jsonb as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    name as name,
    slug as slug,
    NULL as description,
    NULL::timestamp as created_date,
    NULL::timestamp as updated_date,
    NULL as sort_order,
    NULL as template_suffix,
    NULL::float as products_count,
    NULL::float as level_depth,
    NULL as type,
    NULL as published_scope,
    1::boolean as active,
    NULL as parent_category_id
FROM "{{ var("table_prefix") }}_products_categories"