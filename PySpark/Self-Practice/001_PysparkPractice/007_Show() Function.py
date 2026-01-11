# Databricks notebook source
schema = ["id","Name"]
data = [("1","SurajSurajSurajSurajSurajSuraj"),("2","Pandey"),("3","Sanjay"),("4","Shangita"),("5","Atul")]

df = spark.createDataFrame(data=data,schema=schema)
df.show()

# COMMAND ----------

help(df.show)

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

df.show(truncate=8)

# COMMAND ----------

df.show(n=2,truncate=False)

# COMMAND ----------

df.show(n=2,truncate=False, vertical=True)
