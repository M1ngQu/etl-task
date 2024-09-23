


WITH 



latest_transaction as (
    
select max(loaded_timestamp) as max_transaction  from "sales_dw"."dev"."staging_transactions_resellers_csv"

),

  



resellers_csv AS (
  SELECT
    SPLIT_PART(SPLIT_PART(imported_file, '.', '-2'), '_', '-1') :: INT AS reseller_id,
    transaction_id,
    product_name,
    total_amount,
    number_of_purchased_postcards,
    created_date,
    office_location,
    sales_channel,
    loaded_timestamp
  FROM
    "sales_dw"."import"."resellercsv"

      

  -- this filter will only be applied on an incremental run
  where loaded_timestamp > (SELECT max_transaction from latest_transaction LIMIT 1)

  



),
trans_csv AS (
  SELECT
    md5(cast(coalesce(cast(reseller_id as TEXT), '') || '-' || coalesce(cast(transaction_id as TEXT), '') as TEXT)) AS customer_key,
    transaction_id,
    reseller_id,
    product_name,
    total_amount,
    number_of_purchased_postcards,
    created_date,
    office_location,
    sales_channel,
    loaded_timestamp
  FROM
    resellers_csv
)


SELECT
  t.customer_key,
  transaction_id,
  product_key,
  channel_key,
  t.reseller_id,
  to_char(
    created_date,
    'YYYYMMDD'
  ) :: INT AS bought_date_key,
  total_amount::numeric,
  number_of_purchased_postcards,
  e.product_price::numeric,
  e.geography_key,
  s.commission_pct * total_amount::numeric AS commisionpaid,
  s.commission_pct AS commission_pct,
  loaded_timestamp
FROM
  trans_csv t
  JOIN "sales_dw"."dev"."dim_product"
  e
  ON t.product_name = e.product_name
  JOIN "sales_dw"."dev"."dim_channel" C
  ON t.sales_channel = C.channel_name
  JOIN "sales_dw"."dev"."dim_customer"
  cu
  ON t.customer_key = cu.customer_key
  JOIN "sales_dw"."dev"."dim_salesagent"
  s
  ON t.reseller_id = s.original_reseller_id