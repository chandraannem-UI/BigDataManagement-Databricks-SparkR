# Databricks notebook source
library(SparkR)

# COMMAND ----------

# MAGIC %sql
# MAGIC use group3;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iowa_liquor_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct month_of_sale,year_of_sale from iowa_liquor_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iowa_liquor_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct month_of_sale,year_of_sale from iowa_liquor_sales where year_of_sale<2022  or (year_of_sale=2022 and month_of_sale<5)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct month_of_sale,year_of_sale from iowa_liquor_sales where year_of_sale=2022 and month_of_sale>=5

# COMMAND ----------

df_training=sql("select * from iowa_liquor_sales where year_of_sale<2022  or (year_of_sale=2022 and month_of_sale<5)")
df_testing=sql("select * from iowa_liquor_sales where year_of_sale=2022 and month_of_sale>=5")

# COMMAND ----------

df_training=dropna(df_training)
df_testing=dropna(df_testing)

# COMMAND ----------

model_rf = spark.randomForest(df_training, sale_dollars ~ county + category_code + month_of_sale + year_of_sale, "regression", numTrees = 20, maxDepth = 5, handleInvalid = "skip" )
summary(model_rf)

# COMMAND ----------

output = predict(model_rf, df_testing)
showDF(output,20)

# COMMAND ----------

showDF(select(output, alias(avg((output$sale_dollars-output$prediction)^2),'MSE')))

# COMMAND ----------

showDF(select(output, alias(sqrt(avg((output$sale_dollars-output$prediction)^2)),'RMSE')))

# COMMAND ----------

showDF(select(output, alias(avg(abs(output$sale_dollars-output$prediction)),"MAE")))

# COMMAND ----------

showDF(select(output, alias(avg(abs(output$sale_dollars-output$prediction)/abs(output$sale_dollars)), 'MAPE')))
