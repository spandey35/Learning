# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

df_department_spark = spark.read.csv(
    path="/Volumes/workspace/default/pyspark_source/department_spark.csv",
    header=True,
    inferSchema=True
)

df_employee_spark = spark.read.csv(
    path= "/Volumes/workspace/default/pyspark_source/employees_spark.csv",
    header = True,
    inferSchema=True
)

df_salary_spark = spark.read.csv(
    path= "/Volumes/workspace/default/pyspark_source/salary_spark.csv",
    header=True,
    inferSchema= True
)

# COMMAND ----------

df_department_spark.show(2)
df_employee_spark.show(2)
df_salary_spark.show(2)

# COMMAND ----------

df_EmpDetails_Raw = (df_department_spark.join(df_employee_spark, "EmployeeID", "inner")
                 .join(df_salary_spark, "EmployeeID", "inner"))

df_EmpDetails = df_EmpDetails_Raw.select(
                                    df_employee_spark.EmployeeID, 
                                    df_employee_spark.Name,
                                    df_employee_spark.Gender,
                                    df_employee_spark.City,
                                    df_department_spark.Department,
                                    df_department_spark.Location,
                                    df_department_spark.Manager,
                                    df_salary_spark.AnnualSalary,
                                    df_salary_spark.AnnualBonus,
                                    df_salary_spark.NetSalary,
                                    df_salary_spark.Tax)

df_EmpDetails.show(5)                                    

# COMMAND ----------

help(df_employee_spark.filter)

# COMMAND ----------

# DBTITLE 1,Cell 5
## Find employees whose manager name starts with the same letter as their own name.

df_same_initial = df_EmpDetails.filter(
    substring(col("Name"), 1, 1) == substring(col("Manager"), 1, 1)
)
display(df_same_initial)

# COMMAND ----------

## Calculate the average, minimum, and maximum AnnualSalary per department.

df_calcuated = df_EmpDetails.groupBy("Department").agg(
    round(avg("AnnualSalary"),2).alias("Avg_AnnualSalary"),
    round(min("AnnualSalary"),2).alias("Min_AnnualSalary"),
    round(max("AnnualSalary"),2).alias("Max_AnnualSalary")
)

display(df_calcuated)

# COMMAND ----------

## Find the total NetSalary payout per department location.

df_Net_Salary = df_EmpDetails.groupBy("Department").agg(round(sum("NetSalary"),2).alias("Sum_NetSalary"))
df_Net_Salary.display()

# COMMAND ----------

## Determine the department with the highest average AnnualBonus.

HighestBouns = df_EmpDetails.groupBy("Department").agg(round(max("AnnualBonus"),2).alias("Max_AnnualBonus"))
HighestBouns.display()


# COMMAND ----------

## Find the number of employees per manager, but only include managers managing more than 3 employees.

df1 =  df_EmpDetails.groupby("Manager").agg(count("EmployeeID").alias("Emp_Count"))

df2 = (df1
       .filter(df1.Emp_Count > 3 )
       .orderBy(desc("Emp_Count")))

df2.display()


    


# COMMAND ----------

df3 = (df_EmpDetails.
       groupBy("Manager")
       .agg(count("EmployeeID").alias("Emp_Count"))
       .filter( col("Emp_Count") > 3)
       .orderBy(desc("Emp_Count")))

df3.show(5)  

# COMMAND ----------

##Create a new column SalaryGrade:

#A if AnnualSalary less than  100000
#B if between 100001 and 120000
#C otherwise

#Add a column TaxPercentage, calculated as  (Tax / AnnualSalary) * 100.



SalaryGrade = df_EmpDetails.withColumn(
    "SalaryGrade",
    when(col("AnnualSalary") <= 100000, "A")
    .when(col("AnnualSalary") <= 120000, "B")
    .otherwise("C")
)

SalaryGrade.show(5)


TaxPercenatage = df_EmpDetails.withColumn(
    "TaxPercenatage",
    round(((col("Tax") / col("AnnualSalary")) * 100 ),2)
)
TaxPercenatage.show(5)


# COMMAND ----------

## Window Functions 

# 1. Rank employees within each department based on AnnualSalary (highest salary = rank 1).


windowspec  = Window.partitionBy("Department").orderBy(desc("AnnualSalary"))

df_ranked = df_EmpDetails.withColumn("Rank", rank().over(windowspec))
df_ranked.show(5)

# COMMAND ----------

##  Find the top 2 highest-paid employees in each department.

windowspec1 = Window.partitionBy("department").orderBy(desc("department"),desc("AnnualSalary"))

highestPaid_Emp = df_EmpDetails.withColumn("Rank", rank().over(windowspec1))
highestPaid_Emp.filter(col("Rank") <= 2).show(10)

# COMMAND ----------

# DBTITLE 1,Filtering & Complex Conditions
## 1. Retrieve employees whose NetSalary is less than 70% of their AnnualSalary.

df_emp_net_salary = (df_EmpDetails
                     .withColumn("Net Salary Percentage", 
                                round((col("NetSalary") / col("AnnualSalary")) * 100,2)
                                 )
                     .filter(col("Net Salary Percentage") > 70 )
)
df_emp_net_salary.show(5)



## 2. Find employees who receive a bonus greater than 20% of their AnnualSalary.

df_Emp_bonus_details = (
    df_EmpDetails
    .withColumn("Bonus_Percenatge", 
                round((col("AnnualBonus") / col("AnnualSalary")) * 100, 2))
    .filter(col("Bonus_Percenatge") > 20)
    )
df_Emp_bonus_details.show(5)



# COMMAND ----------

## For each manager, find the employee with the highest NetSalary under that manager.

windowspec2 = Window.partitionBy("Manager").orderBy(desc("NetSalary"))

Higest_NetSalary = (df_EmpDetails
                    .withColumn("Highest Net Salary", rank().over(windowspec2))
                    .filter(col("Highest Net Salary") == 1)
                    ) 
display(Higest_NetSalary)                    
