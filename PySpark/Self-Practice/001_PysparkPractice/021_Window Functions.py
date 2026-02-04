# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

dep = spark.read.csv(path="/Volumes/workspace/default/pyspark_source/department_spark.csv",header=True,inferSchema=True)

emp = spark.read.csv(path="/Volumes/workspace/default/pyspark_source/employees_spark.csv",header=True,inferSchema=True)

salary = spark.read.csv(path="/Volumes/workspace/default/pyspark_source/salary_spark.csv",header=True,inferSchema=True)




# COMMAND ----------

dep.show(n=2)
emp.show(n=2)
salary.show(n=2)

# COMMAND ----------

joined_df = (dep
             .join(emp, "EmployeeID", "inner")
             .join(salary, "EmployeeID","inner"))

joined_df.show(n=1)             

# COMMAND ----------

windowSpec1 = Window.partitionBy("City").orderBy(desc("NetSalary"))

df1 = (joined_df
       .select(col("EmployeeID"),round(col("NetSalary"),2).alias("NetSalary")
               ,col("Department"),rank().over(windowSpec1).alias("Window")))

df1_filtered = df1.where(col("Window") < 3).orderBy(col("Department"))
display(df1_filtered)



