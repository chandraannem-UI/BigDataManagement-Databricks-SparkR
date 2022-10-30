-- Databricks notebook source
use group3

-- COMMAND ----------

describe iowa_liquor_sales

-- COMMAND ----------

select count(distinct store_no) as total_num_stores from iowa_liquor_sales

-- COMMAND ----------

select sum(sale_dollars) as sales,month_of_sale,year_of_sale 
from iowa_liquor_sales 
group by month_of_sale,year_of_sale 
order by month_of_sale,year_of_sale

-- COMMAND ----------

select distinct county from iowa_liquor_sales

-- COMMAND ----------

select avg(sale_dollars) as avg_sale,county from iowa_liquor_sales group by county order by avg_sale desc

-- COMMAND ----------

select avg(sale_dollars) as avg_sale,store_location from iowa_liquor_sales group by store_location order by avg_sale desc

-- COMMAND ----------

-- Iowa Liquor Sales by County 
-- Top 20 liquor Bottles sol by Price in a year
-- Top/least 10 liquor stores by Sales in a year
-- Top/least 10 liquor sales by date/season in a year
-- top/least 10 liquor sales by category in a year
-- Top selling Bottole Volume in a year
-- Top selling packs by category ( 6 pack or 24 pack or 12 pack selling more)
-- Top/least Selling products by volume in ltrs/gallons
-- Top liquor selling stores in a year
-- how much sale will happen by catogary every month/week/day <== Predection

-- COMMAND ----------

select distinct(category_name) from iowa_liquor_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Iowa Liquor Sales by County in the last 24 months

-- COMMAND ----------

select sum(sale_dollars) as sales_by_county, county
from iowa_liquor_sales group by county order by sales_by_county desc

-- COMMAND ----------

select count(invoice_line_no) as no_of_invoices_by_county, county
from iowa_liquor_sales group by county order by no_of_invoices_by_county desc

-- COMMAND ----------

select count(distinct store_name) from iowa_liquor_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Top 20 costliest liquor Bottles sold by state in the last 24 months

-- COMMAND ----------

select distinct item_desc,max(bottle_cost) as max_cost
from iowa_liquor_sales 
group by item_desc order by max_cost desc limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Liquor sales for top 10 categories in the last 24 months

-- COMMAND ----------

select sum(sale_dollars) as sales_by_category, category_name
from iowa_liquor_sales group by category_name order by sales_by_category desc limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Liquor sales for top 10 stores in the last 24 months (in which county)

-- COMMAND ----------

select sum(sale_dollars) as sales_by_store, store_name,county
from iowa_liquor_sales group by store_name,county order by sales_by_store desc limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Top selling packs by category ( 6 pack or 24 pack or 12 pack selling more)

-- COMMAND ----------

select count(*) as sales_by_pack,pack from iowa_liquor_sales group by pack order by pack desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Top 10 Selling products by volume(gallons) in the last 24 months

-- COMMAND ----------

select sum(sale_gallons) as gallons_sold,item_desc 
from iowa_liquor_sales
group by item_desc order by gallons_sold desc limit 10
