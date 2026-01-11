# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = [("Suraj",{"hair":"Black","Eye":"Black"}),("Pandey",{"hair":"Brown","Eye":"Brown"})]
schema = ["Name","Properties"]

df= spark.createDataFrame(data,schema)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

schema1 = StructType([
                       StructField("Name",StringType(),nullable=True),
                       StructField("MapType",MapType(StringType(),StringType()),nullable=True)])

# COMMAND ----------

df2 = spark.createDataFrame(data,schema1)
df2.show(truncate=False)
df.printSchema()

# COMMAND ----------

## Accessing the values from MapType()
df1=df.withColumn("Hair",df.Properties["hair"]).withColumn("Eye",df.Properties["Eye"])
df1.show(truncate=False)

# COMMAND ----------

## getIteam() and getfiled()

df3 = df.withColumn(
    "Eye",
    df["Properties"]["Eye"]
).withColumn(
    "hair",
    df["Properties"]["hair"]
)


display(df3)


# COMMAND ----------


