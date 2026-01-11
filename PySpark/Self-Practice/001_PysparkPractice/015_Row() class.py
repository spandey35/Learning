# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

help(Row())

# COMMAND ----------

row = Row("Suraj",2000)
print(row[0] + " " + str(row[1]))

# COMMAND ----------

row1 = Row(name="Suraj",salary=2000)
print(row1.name + " " + str(row1.salary))

# COMMAND ----------

row3 = Row(Name="Suraj",Salary=2000)
row4 = Row(Name="Pandey",Salary=4000)
data = (row3,row4)
df = spark.createDataFrame(data)
df.show()
df.printSchema()

# COMMAND ----------

Person = Row('Name','Age')
Person1 = Person('Suraj',25)
Person2= Person('Pandey',5)

df = spark.createDataFrame([Person1,Person2])
df.show()
df.printSchema()

# COMMAND ----------

## Nested Rows Types

data = [
        Row(Name='Suraj',Properties = Row(Age=30, Gender= 'Male')), \
        Row(Name="Pandey",Properties=  Row(Age=25, Gender = 'Male'))
        ]

df= spark.createDataFrame(data=data)  
df.show()
df.printSchema() 
