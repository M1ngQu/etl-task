
  
    

  create  table "sales_dw"."dev"."dim_customer__dbt_tmp"
  
  
    as
  
  (
    

SELECT 
    customer_key,
    customer_first_name, 
    customer_last_name, 
    customer_email, 
    sales_agent_key
FROM "sales_dw"."dev"."staging_customers"
  );
  