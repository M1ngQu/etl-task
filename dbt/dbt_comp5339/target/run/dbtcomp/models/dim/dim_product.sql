
  
    

  create  table "sales_dw"."dev"."dim_product__dbt_tmp"
  
  
    as
  
  (
    

SELECT 
    product_id AS product_key,
    product_name, 
    geography_key, 
    product_price
FROM "sales_dw"."dev"."staging_product"
  );
  