WITH dates AS (
 
SELECT DISTINCT
DATE(order_date) AS full_date
FROM {{ ref('orders_data_silver') }}
 
)
 
SELECT
 
--Date key
TO_NUMBER(TO_CHAR(full_date,'YYYYMMDD')) AS datekey,
 

full_date,
 

YEAR(full_date) AS year,
 

QUARTER(full_date) AS quarter,
 

MONTH(full_date) AS month,
 
WEEK(full_date) AS week,


DAYNAME(full_date) AS day_of_week,
 
--Holiday Flag(US HOlidays)
CASE
    WHEN TO_CHAR(full_date,'MM-DD') IN ('01-01','07-04','12-25') THEN TRUE
    ELSE FALSE
END AS holiday_flag,
 
--Season
CASE
    WHEN MONTH(full_date) IN (12,1,2) THEN 'Winter'
    WHEN MONTH(full_date) IN (3,4,5,6) THEN 'Spring'
    WHEN MONTH(full_date) IN (7,8,9) THEN 'Summer'
    WHEN MONTH(full_date) IN (10,11) THEN 'Autumn'
END AS season
 
FROM dates