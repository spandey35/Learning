# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = ["id","Name","Salary"]
data = [(1,"suraj","2000"),(2,"Pandey","4000")]

df= spark.createDataFrame(data,schema)
df.show()
df.printSchema()

# COMMAND ----------


schema1 = StructType([
        StructField(name="id",dataType=IntegerType()), \
        StructField(name="Name",dataType=StringType()), \
        StructField(name="Salary",dataType=StringType())
            ])

df2 = spark.createDataFrame(data=data, schema=schema1)
df2.show()
df2.printSchema()

# COMMAND ----------


data1 = [
    (1, ("suraj", "Pandey"), "2000"),
    (2, ("Pandey", "Suraj"), "4000")
]

StructName = StructType([
    StructField("FirstName", StringType(), nullable=True),
    StructField("LastName", StringType(), nullable=True)
])

schema2 = StructType([
    StructField("id", IntegerType(), nullable=True),
    StructField("Name", StructName, nullable=True),
    StructField("Salary", StringType(), nullable=True)
])

df3 = spark.createDataFrame(
    data=data1,
    schema=schema2
)
display(df3)
df3.printSchema()          

