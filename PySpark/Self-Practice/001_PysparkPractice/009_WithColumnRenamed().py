# Databricks notebook source
from pyspark.sql.functions import *

data = [(1,"Suraj","2000"),(2,"Sanjay","3000"),(3,'Pandey',"4000")]
schema= ["id","Name","Salary"]
df= spark.createDataFrame(data=data, schema=schema)
df.show()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df2=df.withColumnRenamed(existing="Salary",new="Salary-Amount")
df.show()
df2.show()
