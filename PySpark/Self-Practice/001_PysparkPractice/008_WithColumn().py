# Databricks notebook source
from pyspark.sql.functions import *

columns = ['id',"Name","Salary"]
data = [(1,"Suraj","2000"),(2,"Sanjay","3000"),(3,"Pandey","4000")]

df = spark.createDataFrame(data=data, schema=columns)
df.show()
df.printSchema()

# COMMAND ----------

help(df.withColumn)

# COMMAND ----------

df1 = df.withColumn(colName="Salary", col=col("Salary").cast('Integer'))
df1.show()
df1.printSchema()

# COMMAND ----------

df2 = df1.withColumn(colName="Salary", col=col("Salary")*2)
df2.show()
df2.printSchema()

# COMMAND ----------

df3 = df2.withColumn(colName="Country", col=lit("India"))
df3.show()
df3.printSchema()

# COMMAND ----------

df4 = df3.withColumn(colName="Copied-Salary", col=col("Salary"))
df4.show()
