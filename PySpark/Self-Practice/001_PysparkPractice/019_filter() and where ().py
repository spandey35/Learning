# Databricks notebook source
data = [(1,"Suraj","M",2000),(2,"Pandey","M",3000),(3,"Shangita","F",4000)]
schema= ("id","Name","Gender","Salary")

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

help(df.filter)

# COMMAND ----------

df.filter(df.Gender == "M").show()
df.filter("Gender == 'M'").show()

df.where(df.Gender=="M").show()
df.where((df.Name=="Suraj") & (df.Salary  == 2000)).show()


# COMMAND ----------


