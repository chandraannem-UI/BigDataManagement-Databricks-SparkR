# Databricks notebook source
library(SparkR)

# COMMAND ----------

library(ggplot2)

# COMMAND ----------

# MAGIC %sql
# MAGIC use group3;

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe iowa_liquor_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select substr(invoice_line_no,0,10),item_desc from iowa_liquor_sales where invoice_line_no like 'INV-332026%';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iowa_liquor_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ## Association Rule Mining

# COMMAND ----------

df_ass=sql("select substr(invoice_line_no,0,10) as OrderID,item_desc from iowa_liquor_sales")

# COMMAND ----------

df_ass=dropna(df_ass)

# COMMAND ----------

df_agg = agg(groupBy(df_ass, df_ass$OrderID), item_desc = "collect_set")
colnames(df_agg)[2] = "items"

# COMMAND ----------

showDF(df_agg)

# COMMAND ----------

fpm = spark.fpGrowth(df_agg, itemsCol="items", minSupport=0.01, minConfidence=0.1)

# COMMAND ----------

freq_df=spark.freqItemsets(fpm)
freq_df_sort=orderBy(freq_df,-freq_df$freq)
showDF(freq_df_sort,10) 

# COMMAND ----------

freq_df_sort_r=collect(freq_df_sort)

# COMMAND ----------

plot_df=head(freq_df_sort_r,20)
barplot(plot_df$freq, names.arg = plot_df$items, xlab = "Items", ylab = "Frequency", las=2)

# COMMAND ----------

ass_df=spark.associationRules(fpm)
showDF(orderBy(ass_df,-ass_df$lift))
