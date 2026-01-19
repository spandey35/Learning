# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = [("1","Suraj"),("2","Pandey")]
df = spark.createDataFrame(data=data, schema=("id","Name"))
df.printSchema()
df.show()

# COMMAND ----------

## Reading the file from Source

df1 = (spark.read.format("csv")
       .option("header",True)
       .option("inferschem",True)
       .load("/Volumes/workspace/default/pyspark_source/dbo-department.csv"))
df1.printSchema()
display(df1)       

# COMMAND ----------


df3 = (
  df1
  .select("Department", "Location")
  .groupBy("Department", "Location")
  .count()
  .orderBy(desc("count"))
)

display(df3)
