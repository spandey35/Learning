# Databricks notebook source
help(spark.read.parquet)

# COMMAND ----------

df = spark.read.parquet(
    '/Volumes/workspace/default/pyspark_source/sample_practice_file.parquet'
)
display(df)
