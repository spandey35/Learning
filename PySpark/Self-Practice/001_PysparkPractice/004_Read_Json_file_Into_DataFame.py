# Databricks notebook source
help(spark.read)

# COMMAND ----------

help(spark.read.json)

# COMMAND ----------

data = [(1,"Suraj"),(2,"Pandey")]
schema = ["id","Name"]

df = spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

help(df.write)

# COMMAND ----------

df.write.json(path="/Volumes/workspace/default/pyspark_destination/JsonData/",mode="overwrite")

# COMMAND ----------

df1=spark.read.json(path='/Volumes/workspace/default/pyspark_destination/JsonData/')
display(df1)

# COMMAND ----------

df.write.json(path="/Volumes/workspace/default/pyspark_destination/JsonData/",mode="append")

# COMMAND ----------

df=spark.read.json(path='/Volumes/workspace/default/pyspark_destination/JsonData/')
display(df)

# COMMAND ----------

df.write.json(path="/Volumes/workspace/default/pyspark_destination/JsonData/",mode="append")



# COMMAND ----------

df2 = spark.read.json(path='/Volumes/workspace/default/pyspark_destination/JsonData/')
display(df2)
