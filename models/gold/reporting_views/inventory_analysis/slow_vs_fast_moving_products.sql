{{ config(materialized='view') }}
 
SELECT
    p.product_name,
    AVG(f.stockturnoverratio) AS turnoverratio,
    CASE
        WHEN AVG(f.stockturnoverratio) > 0.05 THEN 'Fast Moving'
        ELSE 'Slow Moving'
    END AS product_movement 
FROM {{ ref('fact_inventory') }} f
JOIN {{ ref('dim_product') }} p
ON f.productkey = p.productkey
GROUP BY p.product_name