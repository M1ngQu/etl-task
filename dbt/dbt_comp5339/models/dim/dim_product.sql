{{
config(
materialized = 'table',
unique_key = 'product_key'
)
}}

SELECT 
    product_id AS product_key,
    product_name, 
    geography_key, 
    product_price
FROM {{ ref('staging_product') }}