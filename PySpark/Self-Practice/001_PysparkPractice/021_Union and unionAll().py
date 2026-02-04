# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = [(1, "Suraj", "Male", 2000), (2, "Pandey", "Male", 3000), (3, "Pandey-1", "Female", 3000), (4, "Shangita", "Female", 4000)]
schema = ("id", "Name", "Gender", "Salary")

df = spark.createDataFrame(data, schema)


# COMMAND ----------

data1 = [(1, "Suraj", "Male", 2000), (2, "Pandey", "Male", 3000), (3, "Pandey-1", "Female", 3000), (4, "Shangita", "Female", 4000)]
schema1 = ("id", "Name", "Gender", "Salary")

df2 = spark.createDataFrame(data, schema)


# COMMAND ----------

df.show()
df2.show()

# COMMAND ----------

df3 = df.union(df2)
df3.show()

# COMMAND ----------


