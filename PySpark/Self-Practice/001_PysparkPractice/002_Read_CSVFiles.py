# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

help(spark.read)

# COMMAND ----------

df = spark.read.csv(path='/Volumes/workspace/default/pyspark_source/dbo-department.csv',header=True)
display(df)
df.printSchema()

# COMMAND ----------

df_1 = spark.read.format("csv").option("header",True).load("/Volumes/workspace/default/pyspark_source/dbo-department.csv" )
display(df_1)
df_1.printSchema()

# COMMAND ----------

df_2 = spark.read.csv(path=["/Volumes/workspace/default/pyspark_source/"],header=True)
display(df_2)
df_2.printSchema()

# COMMAND ----------

## Defining the Csv file with shecma 


schema = StructType([
    StructField("EmployeeID", IntegerType()),
    StructField("Name", StringType()),
    StructField("City", StringType()),
    StructField("Age", IntegerType()),
    StructField("Gender", StringType()),
    StructField("Created Date", TimestampType()),
    StructField("Modified Date", TimestampType()),
    StructField("IsActive", IntegerType())
])

df_3 = spark.read.csv(
    path=["/Volumes/workspace/default/pyspark_source/"],
    header=True,
    schema=schema
)
df_3.printSchema()
display(df_3)
