{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

WITH variations AS (
    SELECT 
        "{{ var("table_prefix") }}_products".id AS product_id,
        json_agg(json_build_object(
            'id', "{{ var("table_prefix") }}_product_variations".id,
            'price', "{{ var("table_prefix") }}_product_variations".price
        )) AS variation_data
    FROM "{{ var("table_prefix") }}_product_variations"
    INNER JOIN "{{ var("table_prefix") }}_products"
    ON "{{ var("table_prefix") }}_product_variations".id = ANY(
        SELECT jsonb_array_elements_text("{{ var("table_prefix") }}_products".variations)::int
    )
    GROUP BY "{{ var("table_prefix") }}_products".id
)

SELECT 
    NOW() as created,
    NOW() as modified,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_products".id ||
      'product' ||
      'woocommerce'
    )  as id,
    'woocommerce' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_products._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_products".id as external_id,
    "{{ var("table_prefix") }}_products".name as name,
    "{{ var("table_prefix") }}_products".description as description,
    "{{ var("table_prefix") }}_products".short_description as short_description ,
    "{{ var("table_prefix") }}_products".sku as reference,
    "{{ var("table_prefix") }}_products".type as type ,
    "{{ var("table_prefix") }}_products".permalink as url,
    variations as variations,
    "{{ var("table_prefix") }}_products".stock_quantity::float as quantity_available ,
    NULL::float as minimal_quantity ,
    "{{ var("table_prefix") }}_products".stock_status as stock_status ,
    "{{ var("table_prefix") }}_products".images::jsonb as images,
    "{{ var("table_prefix") }}_products".tags::jsonb as tags,
    "{{ var("table_prefix") }}_products".purchasable::boolean as purchasable,
    CASE
   		WHEN "{{ var("table_prefix") }}_products".regular_price = '' THEN NULL::float
   		ELSE ("{{ var("table_prefix") }}_products".regular_price)::float
	  END AS regular_price,
    "{{ var("table_prefix") }}_products".sale_price as sale_price ,
    CASE
   		WHEN "{{ var("table_prefix") }}_products".price = '' THEN NULL::float
   		ELSE ("{{ var("table_prefix") }}_products".price)::float
	  END AS price,
    "{{ var("table_prefix") }}_products".total_sales::float as total_sales,
    "{{ var("table_prefix") }}_products".on_sale::boolean as on_sale ,
    "{{ var("table_prefix") }}_products".average_rating::float as rate ,
    "{{ var("table_prefix") }}_products".slug as slug,
    "{{ var("table_prefix") }}_products".status as status ,
    "{{ var("table_prefix") }}_products".virtual::boolean as virtual ,
    CASE
   		WHEN "{{ var("table_prefix") }}_products".weight = '' THEN NULL::float
   		ELSE ("{{ var("table_prefix") }}_products".weight )::float
	  END AS weight ,
    NULL as ean13 ,
    -- ("{{ var("table_prefix") }}_products".dimensions->>'height')::float as height ,
    -- ("{{ var("table_prefix") }}_products".dimensions->>'width')::float as width ,
    CASE
   		WHEN "{{ var("table_prefix") }}_products".dimensions->>'height' = '' THEN NULL::float
   		ELSE ("{{ var("table_prefix") }}_products".dimensions->>'height')::float
	  END AS height,
    CASE
   	  WHEN "{{ var("table_prefix") }}_products".dimensions->>'width' = '' THEN NULL::float
   		ELSE ("{{ var("table_prefix") }}_products".dimensions->>'width')::float
	  END AS width,
    NULL as location ,
    NULL as manufacturer_name ,
    NULL as unity 
FROM "{{ var("table_prefix") }}_products"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_products
ON _airbyte_raw_{{ var("table_prefix") }}_products._airbyte_ab_id = "{{ var("table_prefix") }}_products"._airbyte_ab_id
LEFT JOIN variations ON "{{ var("table_prefix") }}_products".id = variations.product_id