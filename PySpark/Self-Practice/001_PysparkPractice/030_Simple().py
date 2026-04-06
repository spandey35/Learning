# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.range(start=1 , end= 101)
display(df)

df1 = df.sample(fraction= 0.1)
df1.show()

df2 = df.sample(fraction= 0.1)
df2.show()



# COMMAND ----------

df3 = df.sample(fraction= 0.1, seed=123)
df1.show()

df4 = df.sample(fraction= 0.1, seed=123)
df2.show()
