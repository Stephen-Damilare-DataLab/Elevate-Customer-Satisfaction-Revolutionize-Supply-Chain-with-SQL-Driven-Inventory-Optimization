-- Portfolio project 
-- supply chain analytics project
USE business;

SELECT * FROM external_factors;
DESC external_factors;


-- DATA STANDADIZATION
ALTER TABLE external_factors
MODIFY COLUMN GDP DECIMAL(15,2);

ALTER TABLE external_factors
MODIFY COLUMN inflation_rate DECIMAL(5,2);

ALTER TABLE external_factors
MODIFY COLUMN seasonal_factor DECIMAL(5,2);

-- checking product_information
SELECT * FROM product_information;
desc product_information;

ALTER TABLE product_information
ADD COLUMN Newpromotions ENUM('yes', 'no');

SET SQL_SAFE_UPDATES=0;
UPDATE product_information
SET Newpromotions =
CASE WHEN
Promotions = 'yes' THEN 'yes'
WHEN promotions = 'no' THEN 'no'
ELSE NULL
END; 
ALTER TABLE product_information
DROP COLUMN promotions;

-- standadizing sales_data
SELECT * FROM sales_data;
DESC sales_data;

-- sales_date(text to date), iQ 9(int), product_cost(double to decimal 15,2)

ALTER TABLE sales_data
ADD COLUMN Newsales_date Date;

SET GLOBAL sql_mode = 'NO_ENGINE_SUBSTITUTION';
SELECT @@sql_mode;
 
 set session sql_mode = ' ';

-- changing data datatype
UPDATE sales_data
SET Newsales_date = STR_TO_DATE(sales_date, '%d/%m/%Y');

ALTER TABLE sales_data
DROP COLUMN sales_date;

ALTER TABLE sales_data
CHANGE COLUMN Newsales_date Sales_date date;

-- product_cost (double to decimal(5,2))
ALTER TABLE sales_data
MODIFY COLUMN product_cost DECIMAL(5,2);



use business;
SELECT * FROM external_factors;
-- identifying missing values using is null funtions ON THE EXTERNAL FACTORS DATA
 SELECT 
  SUM(CASE
  WHEN Sales_date is null  THEN 1 ELSE 0 END) AS MISSING_SALE_DATE,
  SUM(CASE
  WHEN GDP is null  THEN 1 ELSE 0 END) AS MISSING_GDP,
  SUM(CASE
  WHEN inflation_rate IS NULL THEN 1 ELSE 0 END) AS MISSING_INFLATION_FACTOR,
  SUM(CASE
  WHEN seasonal_factor IS NULL  THEN 1 ELSE 0 END) AS MISSING_seasional_factors
  FROM external_factors;

-- IDENTIFYING NULL_DATA IN PRODUCT INFORMATION
SELECT * FROM product_information;
SELECT 
SUM(CASE 
WHEN Product_ID IS  NULL THEN 1 ELSE 0 END) AS missing_id,
SUM(CASE
WHEN product_category IS NULL THEN 1 ELSE 0 END ) AS missing_productcategory,
SUM(CASE
WHEN Newpromotions IS NULL THEN 1 ELSE 0 END ) AS missing_newpromotions
FROM product_information;

-- identifying null/ empty field in sales_data


SELECT * FROM sales_data;
SELECT * FROM product_information;
SELECT 
SUM( CASE
WHEN product_ID IS NULL THEN 1 ELSE 0 END ) AS MISSING_PRODUCT_ID, 
SUM(CASE
WHEN inventory_quantity IS NULL THEN 1 ELSE 0 END) AS MISSING_INVENTORY,
SUM(CASE
WHEN product_cost IS NULL THEN 1 ELSE 0 END ) AS MISSING_PRODUCT_COST,
SUM(CASE
WHEN sales_date IS NULL THEN 1 ELSE 0 END ) AS MISSING_SALES_DATE
FROM sales_data;

-- checking for duplicate values on External_factors
SELECT COUNT(*) FROM (
SELECT sales_date, COUNT(*) as count
FROM external_factors
GROUP BY sales_date
HAVING count > 1) dup;


-- DATA INTEGRATION
-- (COMBINING RELEVANT DATASET USING JOINS)
-- COMBINING SALES_DATA AND PRODUCT DATA
CREATE VIEW sales_product_data AS 
SELECT s.product_ID,
s.sales_date,
s.inventory_quantity,
s.product_cost,
prd.product_category,
prd.Newpromotions
FROM sales_data s
JOIN product_information prd
ON s.product_id = prd.product_id;

SELECT * FROM sales_product_data;
select * from external_factors;

-- sales_product data and external_factor

CREATE VIEW inventory_data AS
SELECT sp.product_id,
sp.sales_date,
sp.inventory_quantity,
sp.product_cost,
sp.product_category,
sp.Newpromotions,
ex.GDP,
ex.inflation_rate,
ex.seasonal_factor
FROM sales_product_data sp
LEFT JOIN external_factors ex
ON sp.sales_date = ex.sales_date;
SELECT * FROM  inventory_data ;


-- DESCIPTIVE ANALYSIS
-- CALCULATING AVEGERAGE SALES
select product_id, round(avg(inventory_quantity * product_cost),2) as average_sales
from inventory_data
group by product_id
order by average_sales desc;

-- MEDIAN STOCK LEVEL
SELECT PRODUCT_ID, AVG(INVENTORY_QUANTITY) AS MEDIAN_STOCK FROM(
SELECT PRODUCT_ID, INVENTORY_QUANTITY, ROW_NUMBER() OVER (PARTITION BY PRODUCT_ID ORDER BY INVENTORY_QUANTITY) AS ROW_NUM_ASC,
ROW_NUMBER() OVER(PARTITION BY PRODUCT_ID ORDER BY INVENTORY_QUANTITY DESC) AS ROW_NUM_DESC
FROM inventory_data) subquery 
WHERE ROW_NUM_ASC IN ( ROW_NUM_DESC, ROW_NUM_DESC-1, ROW_NUM_DESC + 1)
GROUP BY PRODUCT_ID
ORDER BY MEDIAN_STOCK DESC;

-- PRODUCT PERFROMANCE METRICS
SELECT product_id,
sum(inventory_quantity * product_cost) AS Total_sales
FROM inventory_data
GROUP BY product_id
ORDER BY Total_sales desc;

-- DETERMINING THE OPTIMAL REORDER POINT FOR EACH PRODUCTS BASED ON HISTORIC DATA SALES DATA AND EXTERNAL FACTOR
-- REORDER TIME = LEAD TIME DEMAND + SAFETY STOCK LEVEL
-- LEAD TIME DEMAND = ROLLING AVERAGE SALE * LEAD TIME
-- SAFETY STOCK = Z * SQUARE ROOT OF LEAD TIME * STANDARD DEVIATION OF  DEMAND
-- Z = 1.645
-- A CONSTANT LEAD TIME OF 7 DAYS
-- TO PROVIDE 95% SERVICE LEVEL

WITH INVENTORY_CALCULATION AS (
SELECT PRODUCT_ID,
AVG(ROLLING_AVG_SALES) AS AVG_ROLLING_SALES,
AVG(ROLLING_VARIANCE) AS AVG_ROLLING_VARIANCE
FROM(
SELECT PRODUCT_ID,
AVG(daily_sales) OVER (PARTITION BY PRODUCT_ID ORDER BY SALES_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW ) AS ROLLING_AVG_SALES,
AVG(Squared_diff) OVER (PARTITION BY PRODUCT_ID ORDER BY SALES_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ROLLING_VARIANCE
FROM ( SELECT PRODUCT_ID,
SALES_DATE,
INVENTORY_QUANTITY * PRODUCT_COST AS daily_sales,
((INVENTORY_QUANTITY * PRODUCT_COST - AVG(INVENTORY_QUANTITY * PRODUCT_COST) OVER (PARTITION BY PRODUCT_ID ORDER BY SALES_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) * (INVENTORY_QUANTITY * PRODUCT_COST - AVG(INVENTORY_QUANTITY * PRODUCT_COST) OVER (PARTITION BY PRODUCT_ID ORDER BY SALES_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW))) AS Squared_diff
FROM INVENTORY_DATA
) SUB1
) SUB2
GROUP BY PRODUCT_ID
)
SELECT PRODUCT_ID,
AVG_ROLLING_SALES * 7 AS LEAD_TIME_DEMAND, 1.645 * (AVG_ROLLING_VARIANCE * 7) AS SAFETY_STOCK,
(AVG_ROLLING_SALES * 7) + (1.645 * (AVG_ROLLING_VARIANCE * 7 )) AS REORDER_POINT
FROM INVENTORY_CALCULATION
;
/*avg_rolling_variance = 0 meaning that there is no deviation from the mean demand of good within the 7 days
Safety stock is extra inventory of an item held to reduce the risk of item stockouts and overpromising to customers.
 more data on each product is needed to calculate the variability over time */
-- creating an inventory optimization table
create table inventory_optimization(
product_id int,
reorder_point double
);

-- creating stored procedure for reorder_point
DELIMITER //
CREATE PROCEDURE RECALCULATE_REORDERPOINT(product_id int)
BEGIN 
 DECLARE avgROLLINGSALES DOUBLE;
 DECLARE avgROLLINGVARIANCE DOUBLE;
 DECLARE LEAD_TIME_DEMAND DOUBLE;
 DECLARE SAFETY_STOCK DOUBLE;
 DECLARE REORDERPOINT DOUBLE;
SELECT
AVG(ROLLING_AVG_SALES) AS AVG_ROLLING_SALES,
AVG(ROLLING_VARIANCE) AS AVG_ROLLING_VARIANCE
INTO avgROLLINGSALES,avgROLLINGVARIANCE
FROM(
SELECT PRODUCT_ID,
AVG(daily_sales) OVER (PARTITION BY PRODUCT_ID ORDER BY SALES_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW ) AS ROLLING_AVG_SALES,
AVG(Squared_diff) OVER (PARTITION BY PRODUCT_ID ORDER BY SALES_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ROLLING_VARIANCE
FROM ( SELECT PRODUCT_ID,
SALES_DATE,
INVENTORY_QUANTITY * PRODUCT_COST AS daily_sales,
((INVENTORY_QUANTITY * PRODUCT_COST - AVG(INVENTORY_QUANTITY * PRODUCT_COST) OVER (PARTITION BY PRODUCT_ID ORDER BY SALES_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) * (INVENTORY_QUANTITY * PRODUCT_COST - AVG(INVENTORY_QUANTITY * PRODUCT_COST) OVER (PARTITION BY PRODUCT_ID ORDER BY SALES_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW))) AS Squared_diff
FROM INVENTORY_DATA
) SUB1
) SUB2
;
SET LEAD_TIME_DEMAND = avgROLLINGSALES * 7;
SET SAFETY_STOCK = (1.645 * SQRT((avgROLLINGVARIANCE) * 7));
SET REORDERPOINT = (avgROLLINGSALES * 7) +  1.645 * SQRT((avgROLLINGVARIANCE) * 7)
;

INSERT INTO inventory_optimization(product_id,reorder_point)
VALUES(PRODUCT_ID, REORDER_POINT)
ON DUPLICATE KEY UPDATE reorder_point =  REORDERPOINT ;
END //
DELIMITER ;
 -- making inventory data a permanent table
 create table inventory_table as select * from inventory_data;
 -- create trigger
 DELIMITER $$
 CREATE TRIGGER AFTERINSERTUNIFIEDTABLE
 AFTER INSERT ON inventory_table
 FOR EACH ROW 
 BEGIN 
  CALL RECALCULATE_REORDERPOINT(NEW.product_id );
  END $$
  DELIMITER ;
  
  
  -- OVERSTOCKING AND UNDERSTOCKING
  WITH ROLLING_SALES AS (
  SELECT PRODUCT_ID, SALES_DATE,AVG(INVENTORY_QUANTITY * PRODUCT_COST) OVER (PARTITION BY PRODUCT_ID ORDER BY SALES_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW ) AS ROLLING_AVG_SALES
  FROM INVENTORY_TABLE
  ),
  
  -- CALCULATING THE NUMBER OF DAY FOR STOCKOUT OF EACH PRODUUCT
  STOCKOUTDAYS AS (
  SELECT PRODUCT_ID,
  COUNT(*) AS STOCK_OUT_DAYS
  FROM INVENTORY_TABLE
  WHERE INVENTORY_QUANTITY = 0
  GROUP BY PRODUCT_ID
  )
  -- JOINING THE ABOVE CTEs TO THE MAIN TABLE TO GET RESULTS
  SELECT F.PRODUCT_ID,
  AVG(F.INVENTORY_QUANTITY * F.PRODUCT_COST) AS AVG_INVENTORY_VALUE,
  AVG(RS.ROLLING_AVG_SALES) AS AVG_ROLLING_SALES,
  COALESCE (SD.STOCK_OUT_DAYS,0) AS STOCK_OUT_DAYS
  FROM inventory_data F
  JOIN  ROLLING_SALES RS ON F.PRODUCT_ID = RS.PRODUCT_ID AND F.SALES_DATE= RS.SALES_DATE
  LEFT JOIN STOCKOUTDAYS SD ON F.PRODUCT_ID =SD.PRODUCT_ID
  GROUP BY F.PRODUCT_ID, SD.STOCK_OUT_DAYS
  ;
  
  -- MONITORING INVENTORY LEVEL
  DELIMITER $$
  CREATE PROCEDURE MONITORINVENTORYLEVELS()
  BEGIN
  SELECT PRODUCT_ID, AVG(INVENTORY_QUANTITY) avginventory
    FROM inventory_table
   GROUP BY PRODUCT_ID
   ORDER BY avginventory DESC;
   END $$
   DELIMITER ;
   
   -- monitor sales trend
   DELIMITER $$
   CREATE PROCEDURE MonitorSalesTrend()
   BEGIN
    SELECT PRODUCT_ID,SALES_DATE,
    AVG(INVENTORY_QUANTITY * PRODUCT_COST) OVER (PARTITION BY PRODUCT_ID ORDER BY SALES_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW ) AS Rolling_avg_sales
    FROM INVENTORY_TABLE
    ORDER BY  PRODUCT_ID,SALES_DATE;
    END $$
    DELIMITER ;
    
-- MONITOR STOCK_OUT FREQUENCY
 DELIMITER $$
   CREATE PROCEDURE MonitorStockOuts()
   BEGIN
    SELECT PRODUCT_ID, COUNT(*) AS StockOutDays
    FROM INVENTORY_TABLE
    WHERE INVENTORY_QUANTITY = 0
    GROUP BY PRODUCT_ID
    ORDER BY StockOutDays DESC;
    END $$
    DELIMITER ;
    
    
    -- FEEDBACK LOOP
    -- REGULARLY COLLECT AND COMPILE FEEDBACKS FROM WORKERS TO IDENTIFY RECURRING ISSUES
    -- ACTIONABLE STEP; ACT ON THE FEEDBACKS TO ADJUST REORDER POINTS AND SAFETY STOCKS LEVELS
    -- DEVELOP A METRIC SYSTEM USING SQL PROCEDURES
    
    
    /*    INSIGHTS and RECOMMENDATION
    -- INSIGHTS
   *  Sales trend is been influenced by external factors eg GDP. Understanding this helps to forecast demands more accurately
    * Inventory dicrepancy; Overstocking and understocking was reveal by the analysis with inconsistency in inventory level
    
   -- RECOMMENDATION
* Optimize reorder point and safety stocks; using the reorder point and safety stock calculated during the analysis to reduce stockout and reduce excess inventory
* Reduce overstock; knowing the product that are overstocked and take actions to reduce the inventory level -- putting the product on discount or promotionak sales
* Take a proactive approach to inventory management by regularly monitoring the key business metrics and making require adjustment to inventory levels, order quantity and safety stock
    */