-- Databricks notebook source
SELECT 
    Brand,
    COUNT(*) as Product_Count,
    ROUND(AVG(Price), 2) as Avg_Price,
    MIN(Price) as Min_Price,
    MAX(Price) as Max_Price,
    ROUND(STDDEV(Price), 2) as Price_StdDev
FROM smartwatch_gold
GROUP BY Brand
ORDER BY Avg_Price DESC;

-- COMMAND ----------

SELECT 
    Price_Category,
    COUNT(*) as Total_Watches,
    ROUND(AVG(Battery_Life), 2) as Avg_Battery,
    SUM(CASE WHEN Call_Function = 'Yes' THEN 1 ELSE 0 END) as With_Call_Function,
    SUM(CASE WHEN Bluetooth = 'Yes' THEN 1 ELSE 0 END) as With_Bluetooth,
    SUM(CASE WHEN GPS = 'Yes' THEN 1 ELSE 0 END) as With_GPS
FROM smartwatch_gold
GROUP BY Price_Category;

-- COMMAND ----------

SELECT 
    Display_Type,
    Battery_Category,
    COUNT(*) as Watch_Count,
    ROUND(AVG(Price), 2) as Avg_Price
FROM smartwatch_gold
WHERE Display_Type IS NOT NULL
GROUP BY Display_Type, Battery_Category
ORDER BY Watch_Count DESC;


-- COMMAND ----------

SELECT 
    Operating_System,
    COUNT(*) as Market_Share,
    ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM smartwatch_gold), 2) as Percentage,
    ROUND(AVG(Price), 2) as Avg_Price
FROM smartwatch_gold
GROUP BY Operating_System
ORDER BY Market_Share DESC;


-- COMMAND ----------

-- Summary by Brand
SELECT 
    Brand,
    COUNT(*) as Product_Count,
    ROUND(AVG(Price), 2) as Avg_Price,
    ROUND(AVG(Battery_Life), 2) as Avg_Battery
FROM smartwatch_gold
GROUP BY Brand
ORDER BY Avg_Price DESC;

-- Call Function Impact
SELECT 
    Call_Function,
    COUNT(*) as Count,
    ROUND(AVG(Price), 2) as Avg_Price,
    ROUND(AVG(Battery_Life), 2) as Avg_Battery
FROM smartwatch_gold
GROUP BY Call_Function;
