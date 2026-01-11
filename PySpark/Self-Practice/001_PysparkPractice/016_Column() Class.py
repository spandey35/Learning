# Databricks notebook source
from pyspark.sql.functions import lit,col

col1= lit ('abcd')
print(type(col1))

# COMMAND ----------

## Creating DataFame

data = [('suraj','male',2000),
        ('Pandey','male',4000)]

schema = ["Name",'Gender','Salary']

df = spark.createDataFrame(data,schema)
df.show()
df.printSchema()

# COMMAND ----------

df1= df.withColumn("NewColumns",lit("NewColumnval"))
df1.show()
df1.printSchema()

# COMMAND ----------

## Accessing the column from datafame
from pyspark.sql.functions import col

df1.select(df1.Gender,df1.Name).show()
df1.select(df1['Gender'],df1["Name"]).show()
df1.select(col("Gender"),col("Name")).show()



# COMMAND ----------

from pyspark.sql.functions import  *
from pyspark.sql.types import *

# COMMAND ----------


data1 = [
    ('suraj', 'male', 2000, ('black','brown')),
    ('Pandey','male', 4000, ('black','blue'))
]

propstruct = StructType([
    StructField('hair', StringType(), nullable=True),
    StructField('Eye', StringType(), nullable=True)
    ])

schema1 = StructType([
    StructField('Name', StringType(), nullable=True), 
    StructField('Gender', StringType(), nullable=True),
    StructField('Salary', IntegerType(), nullable=True),
    StructField('Properties', propstruct, nullable=True)
                    ])

df1 = spark.createDataFrame(data1,schema1)
df1.show()
df1.printSchema()


# COMMAND ----------

## Accessing the Nested Columns 

df1.select(df1.Properties.eye).show()
df1.select(df1['Properties.eye']).show()
df1.select(col("Properties.eye")).show()


