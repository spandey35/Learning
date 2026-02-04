# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = [(1,"Suraj-1","Male","IT"),
        (2,"Suraj-2","Male","IT"),
        (3,"Pandey-1","Female","HR"),
        (4,"Pandey-2","Female","HR"),
        (5,"Shangita-1","Female","IT"),
        (6,"Shangita-2","Female","IT"),
        (7,"Pandey-3","Male","HR"),
        (8,"Sanjay-1","Male","HR"),
        (9,"Sanjay-2","Male","IT")]

schema = ("id","Name","Gender","Department")

df= spark.createDataFrame(data,schema) 
df.show()    

df.groupBy("Gender","Department").count().show()


df.groupBy("Department").pivot("Gender").count().show()


df.groupBy("Department").pivot("Gender",["Male"]).count().show()

# COMMAND ----------

# DBTITLE 1,Unpivot the dataframe
## Unpivot the dataframe

data1 = [("IT",8,6),
         ("Payrol",3,2),
         ("HR",1,1)]

schema1= ("Department","Male","Female")

df1= spark.createDataFrame(data1,schema1)
df1.show()

from pyspark.sql.functions import expr

df1.select("Department", expr("stack(2, 'Male', Male, 'Female', Female) as (Gender, Count)")).show()
