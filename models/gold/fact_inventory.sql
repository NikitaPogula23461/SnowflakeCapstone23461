WITH source_data AS (
 
SELECT
 
    oi.product_id,
    o.store_id,
    TO_DATE(o.order_date) AS order_date,
    SUM(oi.quantity) AS sold_quantity,
 
    product.stock_quantity,
    product.reorder_level,
    product.cost_price,
    product.supplier_id
 
FROM {{ ref('orders_data_silver') }} o
 
INNER JOIN {{ ref('order_items_silver') }} oi
    ON o.order_id = oi.order_id
 
INNER JOIN {{ ref('dim_product') }} product
    ON oi.product_id = product.product_id
 
 
GROUP BY
    oi.product_id,
    o.store_id,
    CAST(o.order_date AS DATE),
    product.stock_quantity,
    product.reorder_level,
    product.cost_price,
    product.supplier_id
 
),
 
transformed AS (
 
SELECT
    ROW_NUMBER() OVER (ORDER BY product_id, store_id, order_date) AS InventoryKey,
 
    ROW_NUMBER() OVER (ORDER BY product_id) AS ProductKey,
 
    ROW_NUMBER() OVER (ORDER BY order_date) AS DateKey,
 
    ROW_NUMBER() OVER (ORDER BY store_id) AS StoreKey,
 
    ROW_NUMBER() OVER (ORDER BY supplier_id) AS SupplierKey,
 
    stock_quantity AS BeginningInventory,
 
    sold_quantity AS SoldQuantity,
 
    reorder_level,
 
    cost_price,
 
    CASE
        WHEN (stock_quantity - sold_quantity) < reorder_level
        THEN reorder_level - (stock_quantity - sold_quantity)
        ELSE 0
    END AS PurchasedQuantity
 
FROM source_data
 
),
 
supplier_totals AS (
 
SELECT
    SupplierKey,
    SUM(PurchasedQuantity) AS SupplierPurchasedQuantity
FROM transformed
GROUP BY SupplierKey
 
),
 
overall_total AS (
 
SELECT
    SUM(PurchasedQuantity) AS TotalPurchasedQuantity
FROM transformed
 
),
 
inventory AS (
 
SELECT
 
    t.InventoryKey,
    t.ProductKey,
    t.DateKey,
    t.StoreKey,
    t.SupplierKey,
 
    t.BeginningInventory,
    t.PurchasedQuantity,
    t.SoldQuantity,
 
    t.BeginningInventory + t.PurchasedQuantity - t.SoldQuantity AS EndingInventory,
 
    (t.BeginningInventory + t.PurchasedQuantity - t.SoldQuantity) * t.cost_price AS InventoryValue,
 
    (t.BeginningInventory + (t.BeginningInventory + t.PurchasedQuantity - t.SoldQuantity)) / 2 AS AverageInventory,
 
    CASE
        WHEN (t.BeginningInventory+(t.BeginningInventory + t.PurchasedQuantity - t.SoldQuantity)) / 2 > 0 THEN t.SoldQuantity /((t.BeginningInventory +(t.BeginningInventory + t.PurchasedQuantity - t.SoldQuantity)) / 2)
        ELSE NULL
    END AS StockTurnoverRatio,
 
    CASE
        WHEN o.TotalPurchasedQuantity > 0 THEN (s.SupplierPurchasedQuantity / o.TotalPurchasedQuantity) * 100
        ELSE 0
    END AS SupplierContributionPercentage
 
FROM transformed t
 
LEFT JOIN supplier_totals s
    ON t.SupplierKey = s.SupplierKey
 
CROSS JOIN overall_total o
 
)
 
SELECT *
FROM inventory