# Databricks notebook source
# spark.sql("select * from silver.activity_log_parsed")

# spark.table("silver.activity_log_parsed").printSchema()

# df = spark.table("silver.activity_log_parsed")
# display((df).count())

# df = spark.sql("truncate table silver.activity_log_parsed")

df = spark.sql("drop table silver.activity_log_parsed")

# COMMAND ----------

df = spark.table("silver.activity_log")
display(df.count())

# COMMAND ----------

df = spark.table("bronze.activity_log_staging")
display(df)

# COMMAND ----------

df = spark.table("control.pipeline_config")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fradulenttransactionmonitoring.control.pipeline_config
# MAGIC SET merge_keys = 'id'
# MAGIC WHERE pipeline_name = 'activity_log' -- This is discarded

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fradulenttransactionmonitoring.control.pipeline_config
# MAGIC SET json_columns = '[]', type_overrides = '{}'
# MAGIC WHERE pipeline_name = 'activity_log' -- Discarded

# COMMAND ----------

df = spark.table("silver.activity_log_parsed")
display(df)