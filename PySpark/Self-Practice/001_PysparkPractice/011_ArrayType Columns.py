# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

data = [("abc", [1,2] ),("mno", [3,4] ),("xyz", [5,6] )]
schema = ["id","Numbers"]

df = spark.createDataFrame(data=data,schema=schema)
df.show()
df.printSchema()

# COMMAND ----------

structure = StructType(
    [StructField("id",StringType(),nullable=True),
     StructField("Numbers",ArrayType(IntegerType()),nullable=True)]
)

df1 = spark.createDataFrame(data=data,schema=structure)
display(df1)
df1.printSchema()

# COMMAND ----------

## Accessing the elements from the df

df1 = df1.withColumn(
    "FirstNumber",
    col("Numbers")[0]
)

display(df1)


# COMMAND ----------

data1 = [(1,2),(3,4)]
schema1 = ["Num1","Num2"]

df3 = spark.createDataFrame(data1,schema1)
df3.show()

df4 = df3.withColumn("AllNumber", array(col("Num1"),col("Num2")))
df4.show()
df.printSchema()
