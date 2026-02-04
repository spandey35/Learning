# Databricks notebook source
from pyspark.sql.functions import *

data = [(1,"Suraj","Male",20000)]
schema = ("Id","Name","Gender","Salary")

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.select(df.Gender,df.Name).show()

# COMMAND ----------


