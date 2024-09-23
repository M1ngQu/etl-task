
  create view "sales_dw"."dev"."src_resellerscsv__dbt_tmp"
    
    
  as (
    WITH raw_resellercsv AS (

    SELECT * FROM "sales_dw"."import"."resellercsv"
)

SELECT transaction_id, product_name, number_of_purchased_postcards, total_amount, sales_channel, customer_first_name, customer_last_name, customer_email, office_location, created_date, imported_file, loaded_timestamp
FROM raw_resellercsv
  );