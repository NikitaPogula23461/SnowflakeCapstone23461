{{ config(materialized='view') }}
 
SELECT
    s.region,
    d.year,
    d.month,
    SUM(f.total_sales_amount) AS total_sales
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_store') }} s
ON f.storekey = s.storekey
JOIN {{ ref('dim_date') }} d
ON f.datekey = d.datekey 
GROUP BY
d.year,
d.month,
s.region
ORDER BY
s.region,
d.year,
d.month