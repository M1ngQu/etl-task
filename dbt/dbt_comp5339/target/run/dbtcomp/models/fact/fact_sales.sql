
  
    

  create  table "sales_dw"."dev"."fact_sales__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    customer_key,
    transaction_id,
    product_key,
    channel_key,
    reseller_id,
    bought_date_key,
    total_amount,
    qty,
    product_price,
    geography_key, 
    commissionpaid,
    commissionpct,
    loaded_timestamp
FROM  "sales_dw"."dev"."staging_transactions"
  );
  