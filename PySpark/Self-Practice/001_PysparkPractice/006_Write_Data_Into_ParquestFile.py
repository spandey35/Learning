# Databricks notebook source
data = [("1","Suraj"),("2","Pandey")]
schema = ["id","Name"]

df = spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

help(df.write.parquet)

# COMMAND ----------

df.write.parquet(path="/Volumes/workspace/default/pyspark_destination/Parquest/", mode='ignore')

# COMMAND ----------

df1 = spark.read.parquet("/Volumes/workspace/default/pyspark_destination/Parquest/")
display(df1)

# COMMAND ----------

df.write.parquet(path="/Volumes/workspace/default/pyspark_destination/Parquest/", mode='append')


# COMMAND ----------

df1 = spark.read.parquet("/Volumes/workspace/default/pyspark_destination/Parquest/")
display(df1)

# COMMAND ----------

df.write.parquet(path="/Volumes/workspace/default/pyspark_destination/Parquest/", mode='overwrite')

# COMMAND ----------

df1 = spark.read.parquet("/Volumes/workspace/default/pyspark_destination/Parquest/")
display(df1)
