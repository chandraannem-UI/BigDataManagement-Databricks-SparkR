# Databricks notebook source
library(SparkR)

# COMMAND ----------

# MAGIC %sql
# MAGIC use group3;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iowa_liquor_sales;

# COMMAND ----------

df_clus = sql("select * from iowa_liquor_sales")


# COMMAND ----------

df_clus=dropna(df_clus)

# COMMAND ----------

model_gm = spark.gaussianMixture(df_clus, ~ sale_dollars + sale_gallons, k = 10, maxIter = 100, tol = 0.01)
summary(model_gm)

# COMMAND ----------

output_gm = predict(model_gm, df_clus)
showDF(output_gm)
