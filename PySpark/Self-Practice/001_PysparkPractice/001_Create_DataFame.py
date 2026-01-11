# Databricks notebook source
## With dir(spark) we can list out all the object avaible in it.
dir(spark)

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

## Type 1 

data = [(1,"Suraj"),(2,"Pandey")]
df = spark.createDataFrame(data=data,schema=["Id", "Name"])
df.show()
df.printSchema()



# COMMAND ----------

from pyspark.sql.types import *
help(StringType)
help(StructField)

# COMMAND ----------

## Type 2
schema = StructType([StructField("id", dataType=IntegerType()),
            StructField("Name", dataType=StringType())])

df1 = spark.createDataFrame (data=data, schema=schema)
df1.show()
df1.printSchema()

# COMMAND ----------


