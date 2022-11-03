-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Making a new directory for group3

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC mkdirs /users/group3

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls /users/

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC ls /datasets/liquor/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Copying liquor data from datasets to group3 folder

-- COMMAND ----------

/*
%fs 
cp "/datasets/liquor/iowa_liquor.csv" "/users/group3/iowa_liquor.csv"
*/

-- COMMAND ----------

-- create database group3;
use group3;

-- COMMAND ----------

DROP TABLE IF EXISTS iowa_liquor_sales_total;

CREATE TABLE IF NOT EXISTS iowa_liquor_sales_total
( invoice_line_no STRING 
, order_date STRING 
, store_no STRING 
, store_name STRING
, address STRING
, city string
, zipcode STRING 
, store_location STRING
, county_number STRING 
, county STRING
, category_code STRING 
, category_name STRING
, vendor_no STRING 
, vendor_name STRING
, itemno STRING 
, item_desc STRING
, pack INT
, bottle_volume_ml INT
, bottle_cost DOUBLE
, bottle_retail DOUBLE
, sale_bottles INT
, sale_dollars DOUBLE
, sale_liters DOUBLE
, sale_gallons DOUBLE
) USING CSV
LOCATION "/users/group3/iowa_liquor.csv"
OPTIONS("header" = "true","escapeQuotes"= "true");

-- COMMAND ----------

describe iowa_liquor_sales_total

-- COMMAND ----------

select count(*) from iowa_liquor_sales_total;

-- COMMAND ----------

select * from iowa_liquor_sales_total

-- COMMAND ----------

select distinct store_name from iowa_liquor_sales_total where store_name like '%,%' and to_date(order_date,'MM/dd/yyyy')>'2020-08-31'

-- COMMAND ----------

to_date(order_date,'MM/dd/yyyy')>'2020-08-31'iowa_liquor_sales_total whereselect count(*) from iowa_liquor_sales_total where to_date(order_date,'MM/dd/yyyy')>'2020-08-31'--5181291

--select distinct to_date(order_date,'MM/dd/yyyy') from iowa_liquor_sales_total where to_date(order_date,'MM/dd/yyyy')>'2020-08-31'

-- COMMAND ----------

select distinct month(to_date(order_date,'MM/dd/yyyy')) from iowa_liquor_sales_total

-- COMMAND ----------

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 10000;

-- COMMAND ----------

DROP TABLE IF EXISTS iowa_liquor_sales;

CREATE TABLE IF NOT EXISTS iowa_liquor_sales
( invoice_line_no STRING,
  order_date DATE, 
  store_no STRING, 
  store_name STRING,
  address STRING,
  city string,
  zipcode STRING, 
  store_location STRING,
  county_number STRING, 
  county STRING,
  category_code STRING,
  category_name STRING,
  vendor_no STRING,
  vendor_name STRING,
  itemno STRING, 
  item_desc STRING,
  pack INT,
  bottle_volume_ml INT,
  bottle_cost DOUBLE,
  bottle_retail DOUBLE,
  sale_bottles INT,
  sale_dollars DOUBLE,
  sale_liters DOUBLE,
  sale_gallons DOUBLE) 
PARTITIONED BY (month_of_sale int,year_of_sale int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' ESCAPED BY '"'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
--ESCAPED BY '"';
--OPTIONS("escapeQuotes"= "true");

-- COMMAND ----------

INSERT
  OVERWRITE TABLE iowa_liquor_sales Partition (month_of_sale, year_of_sale)
SELECT
  invoice_line_no,
  to_date(order_date, 'MM/dd/yyyy'),
  store_no,
  store_name,
  address,
  city,
  zipcode,
  store_location,
  county_number,
  upper(county),
  category_code,
  category_name,
  vendor_no,
  vendor_name,
  itemno,
  item_desc,
  pack,
  bottle_volume_ml,
  bottle_cost,
  bottle_retail,
  sale_bottles,
  sale_dollars,
  sale_liters,
  sale_gallons,
  month(to_date(order_date, 'MM/dd/yyyy')) as month_of_sale,
  year(to_date(order_date, 'MM/dd/yyyy')) as year_of_sale
FROM
  iowa_liquor_sales_total
where
  to_date(order_date, 'MM/dd/yyyy') > '2020-08-31';

-- COMMAND ----------

describe iowa_liquor_sales

-- COMMAND ----------

select distinct month_of_sale,year_of_sale from iowa_liquor_sales --where month_of_sale=2
--where month_of_sale=3 and year_of_sale=2022

-- COMMAND ----------

select count(*) from iowa_liquor_sales

-- COMMAND ----------

select count(distinct store_no) as total_num_store from iowa_liquor_sales
