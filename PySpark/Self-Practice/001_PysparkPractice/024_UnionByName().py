# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

data1 = [(1,"Suraj",23)]
schema1 = ["id","Nam","Age"]

data2= [(1,"Pandey",2000)]
schema2 = ["Id","Name","Salary"]

df1 = spark.createDataFrame(data1,schema1)
df2= spark.createDataFrame(data2,schema2)

display(df1.unionByName(df2, allowMissingColumns=True))

# COMMAND ----------

help(unionByname())
