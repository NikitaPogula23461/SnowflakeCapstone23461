SELECT
 
--Customer Key
ROW_NUMBER() OVER (ORDER BY customer_id, dbt_valid_from) AS customerkey,

customer_id,

--Full name 
first_name || ' ' || last_name AS full_name,
 
valid_email AS email,

valid_phone AS phone,
 
--Address
city,
state,
country,

--Demographic information 
birth_date,
customer_age,
 
customer_segment,
 
registration_date,

--SCD Type 2 tracking 
dbt_valid_from AS start_date,
dbt_valid_to AS end_date,
 
CASE
WHEN dbt_valid_to IS NULL THEN TRUE
ELSE FALSE
END AS is_current
 
FROM {{ ref('customer_data_silver') }}