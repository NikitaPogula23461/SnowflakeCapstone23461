{{ config(materialized='view') }}
 
SELECT
    c.campaign_name,
    SUM(f.new_customers_acquired) AS customers_acquired,
    AVG(f.repeat_purchase_rate) AS average_repeat_purchase_rate
FROM {{ ref('fact_marketingPerformance') }} f
JOIN {{ ref('dim_campaign') }} c
ON f.campaignkey = c.campaignkey
GROUP BY c.campaign_name
ORDER BY customers_acquired DESC