# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

data = [("Suraj", "Pandey-1" ,"Male","IT"),
        ("Sanjay", "Pandey-2","Male","HR"),
        ("Shangita", "Pandey-3","Female","IT")
        ]

schema = ("Name", "LastName","Gender", "Dep")

df = spark.createDataFrame(data=data, schema=schema)

df.write.parquet(path="/Volumes/workspace/default/pyspark_destination/emp",mode="overwrite", partitionBy="Dep")

# COMMAND ----------

df1 = spark.read.parquet(path="/Volumes/workspace/default/pyspark_destination/emp/Dep=HR/")
df1.show()

# COMMAND ----------


