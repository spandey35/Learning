# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

# DBTITLE 1,Data frame Creation
data = [(1, "Suraj", 1000.12, "DE"),
        (2, "Sanjay", 2000.22, "SDE"),
        (3, "Pandey", 3000.32, "")]

Schema = ("Sr.Number", "Name", "Money", "Position")

df = spark.createDataFrame(data=data, schema= Schema)
df.show()

# COMMAND ----------


