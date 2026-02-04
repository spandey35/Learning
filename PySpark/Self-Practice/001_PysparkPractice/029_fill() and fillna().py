# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = [(1,"Suraj","Male",1000,None),(2,"Sanjay","Male",2000,'IT'),(3,"Pandey","Female",4000,"HR"),(4,"Shangita",None,3000,None)]
schema = ("id","Name","Gender","Salary","Department")

df = spark.createDataFrame(data,schema)
df.show()
df.printSchema()

# COMMAND ----------

## fillna ()
df.fillna("Unknown").show()

# COMMAND ----------


