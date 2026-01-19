# Databricks notebook source
from pyspark.sql import *

# COMMAND ----------

help(DataFrameWriter)

# COMMAND ----------

data = [("1","Suraj"),("2","Pandey")]

schema = ["id","Name"]

df=spark.createDataFrame(data=data,schema=schema)
df.printSchema()
display(df)



# COMMAND ----------

help(df.write)

# COMMAND ----------

help(df.write.csv)

# COMMAND ----------

df.write.csv(path="/Volumes/workspace/default/pyspark_destination",header=True,mode='overwrite')

# COMMAND ----------

df1= spark.read.csv(path='/Volumes/workspace/default/pyspark_destination', header=True)
display(df1)
