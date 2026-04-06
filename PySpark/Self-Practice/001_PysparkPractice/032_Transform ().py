# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = [(1,"Suraj",100),(2,"Pandey",200)]
schema = ("id","Name","Salary")

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.withColumn("Name_upper", upper(col("Name"))).show()

# COMMAND ----------

def convrtUpper (df):
    return df.withColumn('Name',upper(df.Name))

# COMMAND ----------

df1 = df.transform(convrtUpper)
df1.show()

# COMMAND ----------


