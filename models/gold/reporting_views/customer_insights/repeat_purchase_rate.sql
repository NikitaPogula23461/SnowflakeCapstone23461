{{ config(materialized='view') }}
 
WITH customer_order_count AS (
    SELECT
        customerkey,
        COUNT(DISTINCT order_id) AS order_count
    FROM {{ ref('fact_sales') }}
    GROUP BY customerkey
)
SELECT
COUNT(CASE WHEN order_count > 1 THEN 1 END) * 100.0/COUNT(1) AS repeat_purchase_rate
FROM customer_order_count   