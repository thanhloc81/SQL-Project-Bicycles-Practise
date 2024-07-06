
---- QUERY 1 ----

-- SELECT 
--     EXTRACT(MONTH from SOD.ModifiedDate) Month,
--     EXTRACT(YEAR from SOD.ModifiedDate) Year,
--     FORMAT_TIMESTAMP('%b %Y', SOD.ModifiedDate) AS Period,
--     PPS.Name,
--     SUM(OrderQty) AS Qty_item,
--     SUM(LineTotal) AS Total_sales,
--     COUNT(DISTINCT SalesOrderID) AS Order_cnt
-- FROM
--     adventureworks2019.Sales.SalesOrderDetail SOD
-- LEFT JOIN
--     adventureworks2019.Production.Product PP 
--         ON SOD.ProductID = PP.ProductID
-- LEFT JOIN
--     adventureworks2019.Production.ProductSubcategory PPS
--         ON PPS.ProductSubcategoryID = CAST(PP.ProductSubcategoryID AS INT)
-- WHERE CAST(SOD.ModifiedDate AS DATE) >= (
--         SELECT 
--             DATE_SUB(CAST(MAX(ModifiedDate) AS DATE), INTERVAL 12 MONTH)
--         FROM
--             adventureworks2019.Sales.SalesOrderHeader
--     )
-- GROUP BY 
--     1, 2, 3, 4
-- ORDER BY    
--     1, 2;
    
-- QUERY 2 ----
-- Year Over Year Growth (YoY) = (Current Period Value � Previous Year�s Value) � 1

-- WITH cte AS (
--   SELECT 
--     EXTRACT(YEAR FROM SOD.ModifiedDate) AS Year,
--     PPS.Name,
--     SUM(OrderQty) AS Qty_item
--   FROM
--     adventureworks2019.Sales.SalesOrderDetail SOD
--   LEFT JOIN 
--     adventureworks2019.Production.Product PP 
--         ON SOD.ProductID = PP.ProductID
--   LEFT JOIN
--     adventureworks2019.Production.ProductSubcategory PPS
--         ON PPS.ProductSubcategoryID = CAST(PP.ProductSubcategoryID AS INT)
--   GROUP BY 
--     1, 2
--   ORDER BY 
--     1 DESC
-- )
-- SELECT 
--   Name,
--   Qty_item,
--   LEAD(qty_item, 1) OVER(PARTITION BY Name ORDER BY year DESC, Name ASC) AS Prv_qty,
--   ROUND((qty_item / LEAD(qty_item, 1) OVER(PARTITION BY Name ORDER BY year DESC, Name ASC)) - 1, 2) AS Qty_diff
-- FROM 
--   cte 
-- ORDER BY 
--   4 DESC
-- LIMIT 
--   3;

-- ---- QUERY 3 ----
-- WITH cte AS (
--   SELECT 
--     EXTRACT(YEAR FROM SOD.ModifiedDate) AS Year,
--     TerritoryID,
--     COUNT(OrderQty) AS Order_cnt
--   FROM 
--         adventureworks2019.Sales.SalesOrderDetail SOD
--   LEFT JOIN 
--         adventureworks2019.Sales.SalesOrderHeader SOH 
--             ON SOD.SalesOrderID = SOH.SalesOrderID
--   GROUP BY 
--     1, 2
-- )
-- SELECT *
-- FROM (
--   SELECT *,
--          DENSE_RANK() OVER(PARTITION BY year ORDER BY order_cnt DESC) AS Rank
--   FROM 
--     cte
-- )
-- WHERE Rank <= 3
-- ORDER BY 
--   1 DESC;


-- ---- Query 4 ---

-- SELECT 
--     EXTRACT(YEAR FROM SOD.ModifiedDate) AS Year,
--     PPS.Name,
--     SUM(UnitPrice * DiscountPct * OrderQty) AS total_cost
-- FROM 
--     adventureworks2019.Sales.SalesOrderDetail SOD
-- LEFT JOIN 
--     adventureworks2019.Production.Product PP
--         ON SOD.ProductID = PP.ProductID
-- LEFT JOIN 
--     adventureworks2019.Production.ProductSubcategory PPS 
--         ON PPS.ProductSubcategoryID = CAST(PP.ProductSubcategoryID AS INT)
-- LEFT JOIN 
--     adventureworks2019.Sales.SpecialOffer SSO 
--         ON SSO.SpecialOfferID = SOD.SpecialOfferID
-- WHERE 
--     Type LIKE 'Seasonal Discount'
-- GROUP BY 
--     1, 2;


-- ---- Query 5 ---
-- WITH total_cus AS (
--   SELECT 
--     EXTRACT(MONTH FROM ModifiedDate) AS month,
--     CustomerID
--   FROM 
--     adventureworks2019.Sales.SalesOrderHeader
--   WHERE 
--     EXTRACT(YEAR FROM ModifiedDate) = 2014
--     AND Status = 5
-- )
-- ,first_buy AS (
--   SELECT 
--     *,
--     ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY month) AS rk
--   FROM 
--     total_cus
-- )
-- SELECT 
--   t2.month month,
--   CONCAT('M-', t1.month - t2.month) SubsequentMonth,
--   COUNT(DISTINCT t2.CustomerID) NumberCustomer
-- FROM 
--   total_cus t1
-- LEFT JOIN 
--   first_buy t2 
--     ON t1.month >= t2.month AND t2.CustomerID = t1.CustomerID
-- GROUP BY 
--   1, 2
-- ORDER BY 
--   1, 2;

-- ---- Query 6 ----
-- WITH cte AS (
--   SELECT 
--     PP.Name,
--     EXTRACT(MONTH FROM PWO.ModifiedDate) AS Month,
--     EXTRACT(YEAR FROM PWO.ModifiedDate) AS Year,
--     SUM(StockedQty) AS Stock_current
--   FROM 
--     adventureworks2019.Production.Product PP
--   LEFT JOIN 
--     adventureworks2019.Production.WorkOrder PWO 
--         ON PP.ProductID = PWO.ProductID
--   WHERE 
--     EXTRACT(YEAR FROM PWO.ModifiedDate) = 2011
--   GROUP BY 
--     1, 2, 3
-- )

-- SELECT 
--   *,
--   CASE 
--     WHEN (Stock_current - Stock_pre) * 100.0 / Stock_pre IS NOT NULL 
--         THEN ROUND((Stock_current - Stock_pre) * 100.0 / Stock_pre, 1)
--     ELSE 0.0 
--   END AS Diff
-- FROM (
--   SELECT 
--     *,
--     LEAD(Stock_current, 1) OVER(PARTITION BY Name ORDER BY Month DESC) AS Stock_pre
--   FROM 
--     cte
-- )
-- ORDER BY 
--   Name;


-- ---- Query 7 ----
-- WITH cte1 AS (
--     SELECT 
--         EXTRACT(MONTH FROM SOD.ModifiedDate) AS Month,
--         EXTRACT(YEAR FROM SOD.ModifiedDate) AS Year,
--         PP.ProductID,
--         PP.Name,
--         SUM(SOD.OrderQty) AS Order_Qty
--     FROM 
--         adventureworks2019.Sales.SalesOrderDetail SOD
--     LEFT JOIN 
--         adventureworks2019.Production.Product PP 
--             ON PP.ProductID = SOD.ProductID
--     WHERE 
--         EXTRACT(YEAR FROM SOD.ModifiedDate) = 2011
--     GROUP BY 
--         1, 2, 3, 4
-- ),
-- cte2 AS (
--     SELECT 
--         EXTRACT(MONTH FROM PWO.ModifiedDate) AS Month,
--         EXTRACT(YEAR FROM PWO.ModifiedDate) AS Year,
--         PWO.ProductID,
--         Name,
--         SUM(StockedQty) AS Stock_Qty
--     FROM 
--         adventureworks2019.Production.WorkOrder PWO
--     LEFT JOIN 
--         adventureworks2019.Production.Product PP 
--             ON PP.ProductID = PWO.ProductID
--     WHERE 
--         EXTRACT(YEAR FROM PWO.ModifiedDate) = 2011
--     GROUP BY 
--         1, 2, 3, 4
-- )

-- SELECT 
--     cte2.Month,
--     cte2.Year,
--     cte2.ProductID,
--     cte2.Name,
--     COALESCE(Order_Qty, 0),
--     COALESCE(Stock_Qty, 0),
--     ROUND(Stock_Qty / NULLIF(Order_Qty, 0), 2) AS Ratio
-- FROM 
--     cte2
-- LEFT JOIN 
--     cte1 ON cte1.ProductID = cte2.ProductID 
--         AND cte1.Month = cte2.Month
-- ORDER BY 
--     1 DESC, 7 DESC;

-- ---- Query 8 ----

-- SELECT 
--     EXTRACT(YEAR FROM ModifiedDate) AS Year,
--     Status,
--     COUNT(DISTINCT PurchaseOrderID) AS Order_cnt,
--     SUM(TotalDue) AS Value
-- FROM 
--     adventureworks2019.Purchasing.PurchaseOrderHeader
-- WHERE 
--     Status = 1 
--     AND EXTRACT(YEAR FROM ModifiedDate) = 2014
-- GROUP BY 
--     1, 2;

