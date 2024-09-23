
  
    

  create  table "sales_dw"."dev"."dim_geography__dbt_tmp"
  
  
    as
  
  (
    

SELECT
      id AS geography_key, 
      cityname AS city_name, 
      countryname AS country_name, 
      regionname AS region_name
FROM
      "sales_dw"."dev_raw"."geography"
  );
  