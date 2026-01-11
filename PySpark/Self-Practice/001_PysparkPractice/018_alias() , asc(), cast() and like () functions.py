# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = [(1,"Suraj",200),(2,"Pandey",300),(3,"Sanjay",400)]
schema = ("id","Name","Salary")
df= spark.createDataFrame(data,schema)



# COMMAND ----------

## alias()

df1=df.select(df.id.alias("Emp_Id"),df.Name.alias("Emp_Name"),df.Salary.alias("Emp_Salary"))
df1.show()

# COMMAND ----------

## asc() and desc()


df2 = df.sort(df.Name.asc(),df.Salary.desc())
df2.show()

# COMMAND ----------

## cast()

df2.printSchema()

df3 = df2.select(df2.id,df2.Name,df2.Salary.cast("int"))
df3.printSchema()


# COMMAND ----------

## like ()

display(
  df3.filter(
    col("name").like("p%")
  )
)



# COMMAND ----------


