# Databricks notebook source
df = spark.table("bronze.activity_log_staging")
df.show(1)