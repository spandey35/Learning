# Databricks notebook source
data = [(1,"Suraj", 2000, 3000), (2,"Pandey",5000,2000)]
schema = ("id","Name","Salary","Bouns")
df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

def TotalPay(x,y):
    return x + y

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

TotalPayment = udf(lambda x, y :TotalPay(x,y), IntegerType())





# COMMAND ----------

df.withColumn("TotalPay", TotalPayment(df.Salary,df.Bouns)).show()

# COMMAND ----------

@udf(returnType=IntegerType())
def TotalPay1(x,y):
    return x + y

df.withColumn("TotalPay", TotalPay1(df.Salary,df.Bouns)).show()

# COMMAND ----------

data = [(1,"Suraj", 2000, 3000), (2,"Pandey",5000,2000)]
schema = ("id","Name","Salary","Bouns")
df = spark.createDataFrame(data,schema)
df.createTempView("Employees")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Employees;
# MAGIC

# COMMAND ----------

spark.udf.register(name='TotalPaysql',f=TotalPay, returnType=IntegerType())

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,TotalPaysql(Salary,Bouns) as TotalPay from Employees;
