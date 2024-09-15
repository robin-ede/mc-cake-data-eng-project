-- SQL Commands to Convert Parquet Files and Analyze Production Rates for Ingredients or Cakes
--
-- Description:
-- This SQL file includes the following:
-- 1. Athena SQL command to create external tables from Parquet files stored in S3.
-- 2. Queries to calculate hourly production rates and cumulative totals for items from the `old_ingredients`, 
--    `new_ingredients`, `old_cakes`, and `new_cakes` tables.
--
-- The Parquet files are converted into Athena tables using the following `CREATE EXTERNAL TABLE` command.
-- Replace `YOUR_S3_PATH` with the actual S3 path containing your Parquet files.
-- 

-- Step 1: Create Athena External Table from Parquet Files

CREATE EXTERNAL TABLE kafka_minecraft.old_ingredients (
    ItemName STRING,
    ItemCount INT,
    Timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://YOUR_S3_PATH/data/old_ingredients/'
TBLPROPERTIES ("parquet.compress"="SNAPPY");

-- Repeat the above `CREATE EXTERNAL TABLE` command for `new_ingredients`, `old_cakes`, and `new_cakes`
-- by changing the table name and S3 path.

-- Step 2: Calculate hourly production rate changes for items
SELECT 
    ItemName,
    date_trunc('hour', Timestamp) AS hour,  -- Group data by hour
    SUM(ItemCount) AS production_per_hour   -- Total production per hour
FROM 
    kafka_minecraft.old_ingredients  -- Replace with new_ingredients, old_cakes, or new_cakes as needed
GROUP BY 
    ItemName,
    date_trunc('hour', Timestamp)
ORDER BY 
    ItemName, hour;

-- Step 3: Calculate total production for each item
SELECT 
    ItemName,
    SUM(ItemCount) AS total_production       -- Total production for each item
FROM 
    kafka_minecraft.old_ingredients  -- Replace with new_ingredients, old_cakes, or new_cakes as needed
GROUP BY 
    ItemName
ORDER BY 
    total_production DESC;

-- Step 4: Compare total production rates between old and new tables (e.g., old_ingredients vs new_ingredients)
SELECT 
    COALESCE(old.ItemName, new.ItemName) AS ItemName,
    
    -- Total production per hour for old_ingredients
    CASE 
        WHEN date_diff('hour', MIN(old.Timestamp), MAX(old.Timestamp)) = 0 
        THEN SUM(old.ItemCount)
        ELSE SUM(old.ItemCount) / date_diff('hour', MIN(old.Timestamp), MAX(old.Timestamp))
    END AS old_production_per_hour,
    
    -- Total production per hour for new_ingredients
    CASE 
        WHEN date_diff('hour', MIN(new.Timestamp), MAX(new.Timestamp)) = 0 
        THEN SUM(new.ItemCount)
        ELSE SUM(new.ItemCount) / date_diff('hour', MIN(new.Timestamp), MAX(new.Timestamp))
    END AS new_production_per_hour
FROM 
    kafka_minecraft.old_ingredients old
FULL OUTER JOIN 
    kafka_minecraft.new_ingredients new
    ON old.ItemName = new.ItemName
GROUP BY 
    COALESCE(old.ItemName, new.ItemName)
ORDER BY 
    old_production_per_hour DESC, new_production_per_hour DESC;

-- Step 5: Compare hourly production rate changes for old vs new ingredients
SELECT 
    COALESCE(old.ItemName, new.ItemName) AS ItemName,
    date_trunc('hour', COALESCE(old.Timestamp, new.Timestamp)) AS hour,
    
    -- Production per hour for old_ingredients
    SUM(old.ItemCount) AS old_production_per_hour,
    
    -- Production per hour for new_ingredients
    SUM(new.ItemCount) AS new_production_per_hour
FROM 
    kafka_minecraft.old_ingredients old
FULL OUTER JOIN 
    kafka_minecraft.new_ingredients new
    ON old.ItemName = new.ItemName AND date_trunc('hour', old.Timestamp) = date_trunc('hour', new.Timestamp)
GROUP BY 
    COALESCE(old.ItemName, new.ItemName),
    date_trunc('hour', COALESCE(old.Timestamp, new.Timestamp))
ORDER BY 
    ItemName, hour;
