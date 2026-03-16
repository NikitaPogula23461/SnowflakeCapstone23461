SELECT
 
 --Store key
ROW_NUMBER() OVER (ORDER BY store_id, dbt_valid_from) AS StoreKey,
 
store_id,
store_name,
 
 --Address
city,
state,
country,

region,
 
store_type,

opening_date,
 
store_size_category,
 
FROM {{ ref('store_data_silver') }}