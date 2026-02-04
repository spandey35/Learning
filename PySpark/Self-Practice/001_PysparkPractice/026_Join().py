# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data1 = (
    [
        (1, "suraj-1", "Male", 2000,1),
        (2, "suraj-2", "Male", 2000,2),
        (3, "suraj-1", "Male", 2000,4)
        ]
    )

schema1 = ("id","Name","Gender","Salary","Depatment-ID") 


data2= ([
        (1,"IT"),
        (2,"HR"),
        (3,"Payrol"),
])

schema2 = ("Depatment-ID","Dept_Name")

df1 = spark.createDataFrame(data1,schema1)
df2 = spark.createDataFrame(data2,schema2)
display(df1)
display(df2)


# COMMAND ----------

## Joins 

join1 = df1.join(df2, df1["Depatment-ID"] == df2["Depatment-ID"], "inner").show()
join2 = df1.join(df2, df1["Depatment-ID"] == df2["Depatment-ID"], "left").show()
join3 = df1.join(df2, df1["Depatment-ID"] == df2["Depatment-ID"], "right").show()
join4 = df1.join(df2, df1["Depatment-ID"] == df2["Depatment-ID"], "full").show()


# COMMAND ----------


