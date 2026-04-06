# Databricks notebook source
data = [(1,"Suraj", 200), (2,"Pandey", 400)]
schema = ("id","Name","Salary")
df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.createOrReplaceTempView("employees")
df1 = spark.sql("Select * from employees")
df1.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select id,upper(Name) as Name from employees;
