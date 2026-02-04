# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data1 = (
    [
        (1, "suraj-1", "Male", 2000,1),
        (2, "suraj-2", "Male", 2000,2),
        (3, "suraj-3", "Male", 2000,4)
        ]
    )

schema1 = ("id","Name","Gender","Salary","Department-ID") 


data2= ([
        (1,"IT"),
        (2,"HR"),
        (3,"Payrol"),
])

schema2 = ("Department-ID","Dept_Name")

df1 = spark.createDataFrame(data1,schema1)
df2 = spark.createDataFrame(data2,schema2)
display(df1)
display(df2)


# COMMAND ----------

df1.join(df2 ,df1["Department-ID"]==df2["Department-ID"],"leftsemi").show()
df1.join(df2 ,df1["Department-ID"]==df2["Department-ID"],"leftanti").show()

# COMMAND ----------

## self join table creation

data3 = [(1,"Suraj-1",0),(2,"Suraj-2",1),(3,"Suraj-3",2),(4,"Suraj-4",2)]
schema3= ("Id","Name","Manager")

dataframe = spark.createDataFrame(data3,schema3)
dataframe.show()


## self join synatx

dataframe.alias("EmpData").join(dataframe.alias("ManagerData"), col("EmpData.Manager")== col("ManagerData.Id") ,"left").show()

# COMMAND ----------


