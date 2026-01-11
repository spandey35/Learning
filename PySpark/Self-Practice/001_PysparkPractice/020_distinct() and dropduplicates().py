# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
 

# COMMAND ----------

data = [(1, "Suraj", "Male", 2000), (2, "Pandey", "Male", 3000), (2, "Pandey", "Male", 3000), (3, "Shangita", "Female", 4000)]
schema = ("id", "Name", "Gender", "Salary")

df = spark.createDataFrame(data, schema)
df.show()

df.distinct().show()

df.dropDuplicates(["Gender", "Salary"]).show()

# COMMAND ----------


