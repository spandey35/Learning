# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = [("suraj",{"hair":"black","eye":"black"}),("pandey",{"hair":"white","eye":"white"})]
schema = StructType([StructField("Name",StringType(),nullable=True),
                     StructField("System",MapType(StringType(),StringType()),nullable=True)])

df = spark.createDataFrame(data,schema)
df.show(truncate=False)
df.printSchema()                     

# COMMAND ----------

## explode()

df_explode = df.select("Name","System",explode("System"))
df_explode.show(truncate=False)


# COMMAND ----------

## map_key()

df_mpa_key = df.withColumn("Keys",map_keys(df.System))
df_mpa_key.show(truncate=False)

# COMMAND ----------

## map_values ()

df_map_values = df.withColumn("values",map_values(df.System))
df_map_values.show(truncate=False)

