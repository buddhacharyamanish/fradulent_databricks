# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS fradulenttransactionmonitoring.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS fradulenttransactionmonitoring.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS fradulenttransactionmonitoring.gold;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop SCHEMA fradulenttransactionmonitoring.bronze;
# MAGIC drop SCHEMA fradulenttransactionmonitoring.silver;
# MAGIC drop SCHEMA fradulenttransactionmonitoring.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS
# MAGIC   fradulenttransactionmonitoring.bronze.fraud_checkpoints;