SELECT

--Product key
ROW_NUMBER() OVER (ORDER BY product_id) AS productkey,
 
product_id,

product_name,
category,
subcategory,
brand,
reorder_level,
 
color,
size,
 
unit_price,
cost_price,
stock_quantity,

supplier_id
FROM {{ ref('product_data_silver') }}