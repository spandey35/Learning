# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
 

# COMMAND ----------

data = [(1, "Suraj", "Male", 2000), (2, "Pandey", "Male", 3000), (3, "Pandey-1", "Female", 3000), (4, "Shangita", "Female", 4000)]
schema = ("id", "Name", "Gender", "Salary")

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df.sort(desc("Salary"),desc("Name")).show()

df.sort(desc("Salary"),desc("Name")).show()

# COMMAND ----------

df.orderBy(desc("Gender")).show()

# COMMAND ----------


