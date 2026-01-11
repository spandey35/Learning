# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = [(1,"Suraj","M",2000),(2,"Pandey","F",3000),(3,"ABCD","",2000)]

schema = ["id","Name","Gender","Salary"]

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

help(when)

# COMMAND ----------

df1 = df.select(df.id, df.Name, when(df.Gender=='M', 'Male').when(df.Gender=='F', 'Female').otherwise('Not Defined').alias('Gender'), df.Salary)
display(df1)

# COMMAND ----------


