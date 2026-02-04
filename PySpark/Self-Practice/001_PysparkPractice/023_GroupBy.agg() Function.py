# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = ([(1, 'Suraj', 'Male', 5000, 'IT'),
         (2, 'Sanjay', 'Male', 7000, 'HR'),
         (3, 'Pandey', 'Female', 2500, 'Payrol'),
         (4, 'Shangita', 'Female', 2500, 'Payrol'),
         (5, 'Madhav', 'Male', 7000, 'IT'),
         (6, 'Atul', 'Male', 8000, 'HR'),
         (7, 'Bharthi', 'Female', 7000, 'Payrol'),
         (8, 'Sudha', 'Female', 1000, 'IT')]
        )

schema = ("Id", "Name", "Gender", "Salary", "Department")

df = spark.createDataFrame(data, schema)
df.printSchema()
df.show()

# COMMAND ----------

dfall = (df.groupBy(df.Department)
         .agg(
            count("Department").alias('Count_Dep'),
            min("Salary").alias("Min_Salary"),
            max("Salary").alias("Max_Salary")
            )
         )
         
dfall.show()

# COMMAND ----------


