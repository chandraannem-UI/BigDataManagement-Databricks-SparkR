# Databricks notebook source
library(SparkR)

# COMMAND ----------

# MAGIC %sql
# MAGIC use group3

# COMMAND ----------

# MAGIC %sql
# MAGIC describe iowa_liquor_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select substr(invoice_line_no,0,10) as invoice_no,order_date,store_no,store_name,city,zipcode,county_number,county,month_of_sale,year_of_sale
# MAGIC ,sum(sale_bottles) as bottle_sales,sum(sale_dollars) as dollar_sales,sum(sale_gallons) as gallons_sales
# MAGIC from iowa_liquor_sales group by invoice_no,order_date,store_no,store_name,city,zipcode,county_number,county,month_of_sale,year_of_sale

# COMMAND ----------

# MAGIC %sql
# MAGIC select avg(dollar_sales) from (select category_name,county_number,county,month_of_sale,year_of_sale
# MAGIC ,sum(sale_bottles) as bottle_sales,sum(sale_dollars) as dollar_sales,sum(sale_gallons) as gallons_sales
# MAGIC from iowa_liquor_sales group by category_name,county_number,county,month_of_sale,year_of_sale)

# COMMAND ----------

# MAGIC %sql
# MAGIC select category_name,county_number,county,month_of_sale,year_of_sale
# MAGIC ,sum(sale_bottles) as bottle_sales,sum(sale_dollars) as dollar_sales,sum(sale_gallons) as gallons_sales
# MAGIC from iowa_liquor_sales group by category_name,county_number,county,month_of_sale,year_of_sale

# COMMAND ----------

# MAGIC %sql
# MAGIC select category_name,county_number,county,month_of_sale,year_of_sale
# MAGIC ,avg(sale_bottles) as bottle_sales,avg(sale_dollars) as dollar_sales,avg(sale_gallons) as gallons_sales
# MAGIC from iowa_liquor_sales 
# MAGIC where year_of_sale=2022 and month_of_sale>=5
# MAGIC group by category_name,county_number,county,month_of_sale,year_of_sale

# COMMAND ----------

# MAGIC %sql
# MAGIC select category_name,county_number,county,month_of_sale,year_of_sale
# MAGIC ,avg(sale_bottles) as bottle_sales,avg(sale_dollars) as dollar_sales,avg(sale_gallons) as gallons_sales
# MAGIC from iowa_liquor_sales 
# MAGIC where year_of_sale=2022 and month_of_sale>=5
# MAGIC group by category_name,county_number,county,month_of_sale,year_of_sale

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   avg(dollar_sales)
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       category_name,
# MAGIC       county_number,
# MAGIC       county,
# MAGIC       month_of_sale,
# MAGIC       year_of_sale,
# MAGIC       avg(sale_bottles) as bottle_sales,
# MAGIC       avg(sale_dollars) as dollar_sales,
# MAGIC       avg(sale_gallons) as gallons_sales
# MAGIC     from
# MAGIC       iowa_liquor_sales
# MAGIC     where
# MAGIC       year_of_sale = 2022
# MAGIC       and month_of_sale >= 5
# MAGIC     group by
# MAGIC       category_name,
# MAGIC       county_number,
# MAGIC       county,
# MAGIC       month_of_sale,
# MAGIC       year_of_sale
# MAGIC   )

# COMMAND ----------


df_training = sql("select category_name,county_number,county,month_of_sale,year_of_sale
,avg(sale_bottles) as bottle_sales,avg(sale_dollars) as dollar_sales,avg(sale_gallons) as gallons_sales
from iowa_liquor_sales 
where year_of_sale<2022  or (year_of_sale=2022 and month_of_sale<5)
group by category_name,county_number,county,month_of_sale,year_of_sale")


df_testing = sql("select category_name,county_number,county,month_of_sale,year_of_sale
,avg(sale_bottles) as bottle_sales,avg(sale_dollars) as dollar_sales,avg(sale_gallons) as gallons_sales
from iowa_liquor_sales 
where year_of_sale=2022 and month_of_sale>=5
group by category_name,county_number,county,month_of_sale,year_of_sale")


# COMMAND ----------

df_training=dropna(df_training)
df_testing=dropna(df_testing)

# COMMAND ----------

df_training$target_sale = ifelse(df_training$dollar_sales>133,1,0)
df_testing$target_sale = ifelse(df_testing$dollar_sales>133,1,0)

# COMMAND ----------

model_log <- spark.logit(df_training, target_sale ~ category_name +county +month_of_sale +year_of_sale, regParam = 0.2, family = "auto", thresholds = 0.5, handleInvalid = "skip")

# COMMAND ----------

summary(model_log)

# COMMAND ----------


output <- predict(model_log, df_testing)
showDF(output, 100)

# COMMAND ----------

Correct = nrow(where(output, output$target_sale == output$prediction))
Total = nrow(output)
Accuracy = Correct/Total
Accuracy

# COMMAND ----------

TP = nrow(where(output, output$target_sale == 1 & output$prediction == 1))
FP = nrow(where(output, output$target_sale == 0 & output$prediction == 1))
FN = nrow(where(output, output$target_sale == 1 & output$prediction == 0))
Precision = TP/(TP+FP)
Recall = TP/(TP+FN)
F1 = 2*((Precision*Recall)/(Precision+Recall))


# COMMAND ----------

print(Accuracy)
print(Precision)
print(Recall)
print(F1)

# COMMAND ----------

sprintf("Accuracy is: %f",Accuracy)

# COMMAND ----------

sprintf("Precision is: %f",Precision)

# COMMAND ----------

sprintf("Recall is: %f",Recall)

# COMMAND ----------

sprintf("F1 is: %f",F1)
