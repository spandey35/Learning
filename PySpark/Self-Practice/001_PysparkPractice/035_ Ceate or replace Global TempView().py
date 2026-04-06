# Databricks notebook source
data = [(1,"Suraj", 200), (2,"Pandey", 400)]
schema = ("id","Name","Salary")
df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df1 = df.createGlobalTempView("GloEmployee")
df1.show()

# COMMAND ----------


