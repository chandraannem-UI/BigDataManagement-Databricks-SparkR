-- Databricks notebook source
use group3

-- COMMAND ----------

describe iowa_liquor_sales

-- COMMAND ----------

select count(distinct store_no) from iowa_liquor_sales

-- COMMAND ----------

select sum(sale_dollars) as sales,month_of_sale,year_of_sale from iowa_liquor_sales group by month_of_sale,year_of_sale order by month_of_sale,year_of_sale

-- COMMAND ----------


