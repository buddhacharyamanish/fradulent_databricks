# Databricks notebook source
from pyspark.sql.functions import col, column
df = spark.table("bronze.activity_log_staging").orderBy("id")
# df = df.select(col("payload"))
df.show(1, truncate=False)


# COMMAND ----------

from pyspark.sql.functions import col, column, first

source_table = "fradulenttransactionmonitoring.bronze.activity_log_staging"
df_bronze = spark.table(source_table)
# df_bronze.select("id", "event_type", "payload").orderBy(col("id").desc()).show(5, truncate=False)

# checking the structure of the JSON payload for all event types

(df_bronze.where(col("payload").isNotNull())
       .groupBy("event_type")
       .agg(first("payload").alias("sample_payload"))
       .show(truncate=False))

# COMMAND ----------

from pyspark.sql import functions as F

SRC = "fradulenttransactionmonitoring.bronze.activity_log_staging"
base_cols = ["id", "event_id", "event_type", "event_time", "user_id",
             "account_id", "transaction_id", "severity", "created_at"]   

df = spark.table(SRC).select(*base_cols, "payload")

payload_ddl = (df.where(F.col("payload").isNotNull()).limit(1000)
                 .agg(F.expr("schema_of_json_agg(payload)")).first()[0])

parsed = df.withColumn("p", F.from_json("payload", payload_ddl))
parsed.select("p").printSchema()      # Check the nested schemema

# COMMAND ----------

df = (spark.read.format("kafka")
      .option("kafka.bootstrap.servers", "34.228.32.86:9092")
      .option("subscribe", "frauddb.frauddb.activity_log")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load())
print(df.count())
df.selectExpr("CAST(value AS STRING) AS v").show(1, truncate=False)