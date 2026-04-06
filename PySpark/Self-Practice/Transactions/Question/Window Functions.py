# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.table("workspace.dbo.transactioncustomermaster")
df.show(1)

# COMMAND ----------

customer_schema =  StructType([
                             StructField("customer_id", IntegerType(), True) ,
                              StructField("customer_name", StringType(), True) ,
                              StructField("gender", StringType(), True ),
                              StructField("age", IntegerType(), True ),
                              StructField("city", StringType(), True ),
                              StructField("signup_date", TimestampType(), True)]) 
transformed = df

for field in customer_schema.fields:
    casted_1 = transformed.withColumn(field.name,
                                      col(field.name)
                                      .cast(field.dataType))
display(casted_1)


# COMMAND ----------

window_spec = Window.partitionBy("gender").orderBy("city")

res = casted_1.withColumn("Value",rank().over(window_spec))
res1= res.groupBy("Value").agg(count("*").alias("Count"))
display(res1)
