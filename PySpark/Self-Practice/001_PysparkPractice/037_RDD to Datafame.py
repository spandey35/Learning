# Databricks notebook source
data = [(1,"Suraj"), (2,"Pandey")]
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())

# COMMAND ----------


