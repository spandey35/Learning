# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = [(1,"Suraj",["Data Enginer","Python"]),(2,"Pandey",["Java","Data Enginer"])]
schema = ["id","Name","Skills"]

df = spark.createDataFrame(data,schema)
display(df)
df.printSchema()

# COMMAND ----------

## expload() function

df.show(truncate=False)
df1=df.withColumn("New-Skills",explode(col("Skills")))
df1.show(truncate=False)
display(df1)


# COMMAND ----------

## split()

data1 = [(1,"Suraj","Data Enginer,Python"),(2,"Pandey","Java,Data Enginer")]
schema1 = ["id","Name","Skills"]

df3_split = spark.createDataFrame(data1,schema1)
df3_split.show(truncate=False)

#############
print("Output: ")
df3 = df3_split.withColumn("SplitedSkills",split(col("Skills"),","))
df3.show(truncate=False)
df3.printSchema()


# COMMAND ----------

## array()

data3= [(1,"Suraj","Data Enginer","Python"),(2,"Pandey","Java,Data", "Enginer")]
schema3 = ["id","Name","Pri-Skills","Sec-Skills"]

df_array = spark.createDataFrame(data3,schema3)
df_array.show(truncate=False)


#############
print("Output: ")
df_array_output = df_array.withColumn("Total-Skills",array(col("Pri-Skills"),col("Sec-Skills")))
df_array_output.show(truncate=False)
df_array_output.printSchema()


# COMMAND ----------

## array_Cantains()

data4= [(1,"Suraj",["Data Enginer","Python"]),(2,"Pandey",["Java", "Enginer"])]
schema4 = ["id","Name","Skills"]

df_array_catain = spark.createDataFrame(data4,schema4)
df_array_catain.show(truncate=False)


#############
print("Output: ")
df_array_catain_output = df_array_catain.withColumn("Has Java Skills",array_contains(col("Skills"),"Java"))
df_array_catain_output.show(truncate=False)
df_array_catain_output.printSchema()
