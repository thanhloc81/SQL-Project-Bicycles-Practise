# SQL-Project-Bicycles-Practise
✨ Using SQL to extract data following a simulated task

[Link BigQuery](https://console.cloud.google.com/bigquery?sq=400862878778:f171ee5c3bd14daebaf2efc24e417309)
## Dataset
- Utilizing the Adventure Works dataset, which consists of two main modules: Sales and Product.
![image](https://github.com/thanhloc81/SQL-Project-Bicycles-Practise/assets/151768013/8511b949-1e13-4e47-8552-5f25303081e6)![image](https://github.com/thanhloc81/SQL-Project-Bicycles-Practise/assets/151768013/8e19a434-ef19-4a63-b745-3764225890f3)

## Tools
- Google BigQuery

## Practise
### Question 1: Calc Quantity of items, Sales value & Order quantity by each Subcategory in L12M
``````sql
SELECT 
    EXTRACT(MONTH from SOD.ModifiedDate) Month,
    EXTRACT(YEAR from SOD.ModifiedDate) Year,
    FORMAT_TIMESTAMP('%b %Y', SOD.ModifiedDate) AS Period,
    PPS.Name,
    SUM(OrderQty) AS Qty_item,
    SUM(LineTotal) AS Total_sales,
    COUNT(DISTINCT SalesOrderID) AS Order_cnt
FROM
    adventureworks2019.Sales.SalesOrderDetail SOD
LEFT JOIN
    adventureworks2019.Production.Product PP 
        ON SOD.ProductID = PP.ProductID
LEFT JOIN
    adventureworks2019.Production.ProductSubcategory PPS
        ON PPS.ProductSubcategoryID = CAST(PP.ProductSubcategoryID AS INT)
WHERE CAST(SOD.ModifiedDate AS DATE) >= (
        SELECT 
            DATE_SUB(CAST(MAX(ModifiedDate) AS DATE), INTERVAL 12 MONTH)
        FROM
            adventureworks2019.Sales.SalesOrderHeader
    )
GROUP BY 
    1, 2, 3, 4
ORDER BY    
    1, 2;
``````
#### Results
![image](https://github.com/thanhloc81/SQL-Project-Bicycles-Practise/assets/151768013/909680a8-6df7-4e88-98bf-bf0f8265e11b)

### Question 2: Calc % YoY growth rate by SubCategory & release top 3 cat with highest grow rate. Round results to 2 decimal
``````sql
WITH cte AS (
  SELECT 
    EXTRACT(YEAR FROM SOD.ModifiedDate) AS Year,
    PPS.Name,
    SUM(OrderQty) AS Qty_item
  FROM
    adventureworks2019.Sales.SalesOrderDetail SOD
  LEFT JOIN 
    adventureworks2019.Production.Product PP 
        ON SOD.ProductID = PP.ProductID
  LEFT JOIN
    adventureworks2019.Production.ProductSubcategory PPS
        ON PPS.ProductSubcategoryID = CAST(PP.ProductSubcategoryID AS INT)
  GROUP BY 
    1, 2
  ORDER BY 
    1 DESC
)
SELECT 
  Name,
  Qty_item,
  LEAD(qty_item, 1) OVER(PARTITION BY Name ORDER BY year DESC, Name ASC) AS Prv_qty,
  ROUND((qty_item / LEAD(qty_item, 1) OVER(PARTITION BY Name ORDER BY year DESC, Name ASC)) - 1, 2) AS Qty_diff
FROM 
  cte 
ORDER BY 
  4 DESC
LIMIT 
  3;
``````
#### Results
![image](https://github.com/thanhloc81/SQL-Project-Bicycles-Practise/assets/151768013/9334276f-378e-48d1-b464-4750dc8e70db)

### Question 3: Ranking Top 3 TeritoryID with biggest Order quantity of every year. If there's TerritoryID with same quantity in a year, do not skip the rank number
``````sql
WITH cte AS (
  SELECT 
    EXTRACT(YEAR FROM SOD.ModifiedDate) AS Year,
    TerritoryID,
    COUNT(OrderQty) AS Order_cnt
  FROM 
        adventureworks2019.Sales.SalesOrderDetail SOD
  LEFT JOIN 
        adventureworks2019.Sales.SalesOrderHeader SOH 
            ON SOD.SalesOrderID = SOH.SalesOrderID
  GROUP BY 
    1, 2
)
SELECT *
FROM (
  SELECT *,
         DENSE_RANK() OVER(PARTITION BY year ORDER BY order_cnt DESC) AS Rank
  FROM 
    cte
)
WHERE Rank <= 3
ORDER BY 
  1 DESC;
``````
#### Results
![image](https://github.com/thanhloc81/SQL-Project-Bicycles-Practise/assets/151768013/b6e01fd6-e9a2-43d9-bafd-f964a99e9bcf)

### Question 4: Calc Total Discount Cost belongs to Seasonal Discount for each SubCategory
``````sql
SELECT 
    EXTRACT(YEAR FROM SOD.ModifiedDate) AS Year,
    PPS.Name,
    SUM(UnitPrice * DiscountPct * OrderQty) AS total_cost
FROM 
    adventureworks2019.Sales.SalesOrderDetail SOD
LEFT JOIN 
    adventureworks2019.Production.Product PP
        ON SOD.ProductID = PP.ProductID
LEFT JOIN 
    adventureworks2019.Production.ProductSubcategory PPS 
        ON PPS.ProductSubcategoryID = CAST(PP.ProductSubcategoryID AS INT)
LEFT JOIN 
    adventureworks2019.Sales.SpecialOffer SSO 
        ON SSO.SpecialOfferID = SOD.SpecialOfferID
WHERE 
    Type LIKE 'Seasonal Discount'
GROUP BY 
    1, 2;

``````
#### Results
![image](https://github.com/thanhloc81/SQL-Project-Bicycles-Practise/assets/151768013/c4f88e23-77f8-40ce-be8f-991219fd8ee7)

### Question 5: Retention rate of Customer in 2014 with status of Successfully Shipped (Cohort Analysis)
``````sql
WITH total_cus AS (
  SELECT 
    EXTRACT(MONTH FROM ModifiedDate) AS month,
    CustomerID
  FROM 
    adventureworks2019.Sales.SalesOrderHeader
  WHERE 
    EXTRACT(YEAR FROM ModifiedDate) = 2014
    AND Status = 5
)
,first_buy AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY month) AS rk
  FROM 
    total_cus
)
SELECT 
  t2.month month,
  CONCAT('M-', t1.month - t2.month) SubsequentMonth,
  COUNT(DISTINCT t2.CustomerID) NumberCustomer
FROM 
  total_cus t1
LEFT JOIN 
  first_buy t2 
    ON t1.month >= t2.month AND t2.CustomerID = t1.CustomerID
GROUP BY 
  1, 2
ORDER BY 
  1, 2;
``````
#### Results
![image](https://github.com/thanhloc81/SQL-Project-Bicycles-Practise/assets/151768013/47b76fd9-3b97-45e6-a9ed-b920b3ce34e9)

### Question 6: Trend of Stock level & MoM diff % by all product in 2011. If %gr rate is null then 0. Round to 1 decimal
``````sql
WITH cte AS (
  SELECT 
    PP.Name,
    EXTRACT(MONTH FROM PWO.ModifiedDate) AS Month,
    EXTRACT(YEAR FROM PWO.ModifiedDate) AS Year,
    SUM(StockedQty) AS Stock_current
  FROM 
    adventureworks2019.Production.Product PP
  LEFT JOIN 
    adventureworks2019.Production.WorkOrder PWO 
        ON PP.ProductID = PWO.ProductID
  WHERE 
    EXTRACT(YEAR FROM PWO.ModifiedDate) = 2011
  GROUP BY 
    1, 2, 3
)

SELECT 
  *,
  CASE 
    WHEN (Stock_current - Stock_pre) * 100.0 / Stock_pre IS NOT NULL 
        THEN ROUND((Stock_current - Stock_pre) * 100.0 / Stock_pre, 1)
    ELSE 0.0 
  END AS Diff
FROM (
  SELECT 
    *,
    LEAD(Stock_current, 1) OVER(PARTITION BY Name ORDER BY Month DESC) AS Stock_pre
  FROM 
    cte
)
ORDER BY 
  Name;

``````
#### Results
![image](https://github.com/thanhloc81/SQL-Project-Bicycles-Practise/assets/151768013/35ef7e9b-16ab-47eb-a730-448cc6e46ede)

### Question 7: Calc Ratio of Stock / Sales in 2011 by product name, by month. Order results by month desc, ratio desc. Round Ratio to 1 decimal mom yoy
``````sql
WITH cte1 AS (
    SELECT 
        EXTRACT(MONTH FROM SOD.ModifiedDate) AS Month,
        EXTRACT(YEAR FROM SOD.ModifiedDate) AS Year,
        PP.ProductID,
        PP.Name,
        SUM(SOD.OrderQty) AS Order_Qty
    FROM 
        adventureworks2019.Sales.SalesOrderDetail SOD
    LEFT JOIN 
        adventureworks2019.Production.Product PP 
            ON PP.ProductID = SOD.ProductID
    WHERE 
        EXTRACT(YEAR FROM SOD.ModifiedDate) = 2011
    GROUP BY 
        1, 2, 3, 4
),
cte2 AS (
    SELECT 
        EXTRACT(MONTH FROM PWO.ModifiedDate) AS Month,
        EXTRACT(YEAR FROM PWO.ModifiedDate) AS Year,
        PWO.ProductID,
        Name,
        SUM(StockedQty) AS Stock_Qty
    FROM 
        adventureworks2019.Production.WorkOrder PWO
    LEFT JOIN 
        adventureworks2019.Production.Product PP 
            ON PP.ProductID = PWO.ProductID
    WHERE 
        EXTRACT(YEAR FROM PWO.ModifiedDate) = 2011
    GROUP BY 
        1, 2, 3, 4
)

SELECT 
    cte2.Month,
    cte2.Year,
    cte2.ProductID,
    cte2.Name,
    COALESCE(Order_Qty, 0) Total_Order_Qty,
    COALESCE(Stock_Qty, 0) Total_Stock_Qty,
    ROUND(Stock_Qty / NULLIF(Order_Qty, 0), 2) AS Ratio
FROM 
    cte2
LEFT JOIN 
    cte1 ON cte1.ProductID = cte2.ProductID 
        AND cte1.Month = cte2.Month
ORDER BY 
    1 DESC, 7 DESC;
``````
#### Results
![image](https://github.com/thanhloc81/SQL-Project-Bicycles-Practise/assets/151768013/04c1a7d7-beee-4fc5-9d09-9f6f038e6c80)






