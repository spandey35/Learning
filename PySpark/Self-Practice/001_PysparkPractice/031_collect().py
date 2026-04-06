# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = [(1, "Suraj", "Male", 1000), (2, "Sanjay", "Male", 2000)]
schema = ("id", "Name","Gender", "Salary")

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

listrows= df.collect()
print(listrows)
print(listrows[0][1])

# COMMAND ----------


