# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = [(1, "Suraj", ["Java", "Python"]), (2,"Pandey",["Java-1","Python-2"])]
schema = ("id","Name","Skills")

df = spark.createDataFrame(data, schema)
df.show()




# COMMAND ----------

df1 = df.select("id","Name", transform("Skills", lambda x: upper(x)).alias("Skills"))
df1.show()

# COMMAND ----------

def convertupper(x):
    return upper(x)

df.select("Name","id",transform("Skills",convertupper).alias("Skills")).show()
