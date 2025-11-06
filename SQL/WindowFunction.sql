-- 1. Rank Function--

WITH CET AS
(
    SELECT 
        P.ProductID,
        COUNT(A.ProductID) AS Product_Count,
        SUM(A.UnitPrice) AS Sum_UnitPrice,
        RANK() OVER (ORDER BY SUM(A.UnitPrice) DESC) AS RNK
    FROM SalesLT.SalesOrderDetail A
    INNER JOIN SalesLT.Product P 
        ON A.ProductID = P.ProductID
    GROUP BY P.ProductID
)
SELECT TOP 5 
    ProductID,
    Product_Count,
    Sum_UnitPrice,
    RNK
FROM CET
ORDER BY Sum_UnitPrice DESC;


-- 2. ROW_NUMBER()

WITH CTE AS
(
    SELECT 
        P.ProductID,
        SUM(A.UnitPrice) AS TotalPrice,
        ROW_NUMBER() OVER (ORDER BY SUM(A.UnitPrice) DESC) AS RowNo
    FROM SalesLT.SalesOrderDetail A
    INNER JOIN SalesLT.Product P ON A.ProductID = P.ProductID
    GROUP BY P.ProductID
)
SELECT *
FROM CTE
WHERE RowNo <= 5;



-- 3. DENSE_RANK()

WITH CTE AS
(
    SELECT 
        P.ProductID,
        SUM(A.UnitPrice) AS TotalPrice,
        DENSE_RANK() OVER (ORDER BY SUM(A.UnitPrice) DESC) AS DenseRnk
    FROM SalesLT.SalesOrderDetail A
    INNER JOIN SalesLT.Product P ON A.ProductID = P.ProductID
    GROUP BY P.ProductID
)
SELECT *
FROM CTE;


-- 4. 

SELECT 
    P.ProductID,
    SUM(A.UnitPrice) AS TotalPrice,
    NTILE(4) OVER (ORDER BY SUM(A.UnitPrice) DESC) AS PriceCategory
FROM SalesLT.SalesOrderDetail A
INNER JOIN SalesLT.Product P ON A.ProductID = P.ProductID
GROUP BY P.ProductID;


-- .5. SUM() OVER – Running Total

SELECT 
    SalesOrderID,
    ProductID,
    UnitPrice,
    SUM(UnitPrice) OVER (ORDER BY SalesOrderID) AS RunningTotal
FROM SalesLT.SalesOrderDetail;


-- 6. AVG() OVER – Moving Average

SELECT 
    ProductID,
    UnitPrice,
    AVG(UnitPrice) OVER (PARTITION BY ProductID) AS AvgUnitPriceByProduct
FROM SalesLT.SalesOrderDetail;

-- 7. LEAD() – Next Row Value

SELECT 
    SalesOrderID,
    ProductID,
    UnitPrice,
    LEAD(UnitPrice) OVER (ORDER BY SalesOrderID) AS NextOrderPrice
FROM SalesLT.SalesOrderDetail;


-- 8. LAG() – Previous Row Value
SELECT 
    SalesOrderID,
    ProductID,
    UnitPrice,
    LAG(UnitPrice) OVER (ORDER BY SalesOrderID) AS PreviousOrderPrice
FROM SalesLT.SalesOrderDetail;


-- 9. FIRST_VALUE() / LAST_VALUE()

SELECT 
    ProductID,
    UnitPrice,
    FIRST_VALUE(UnitPrice) OVER (ORDER BY UnitPrice ASC) AS CheapestPrice,
    LAST_VALUE(UnitPrice) OVER (ORDER BY UnitPrice ASC
         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS CostliestPrice
FROM SalesLT.SalesOrderDetail;





