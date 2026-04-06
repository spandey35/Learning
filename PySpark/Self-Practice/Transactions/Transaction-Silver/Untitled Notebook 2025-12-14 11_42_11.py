# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# COMMAND ----------

Customer_Brozne = spark.read.table("workspace.dbo.transactioncustomermaster")

# COMMAND ----------

from pyspark.sql.functions import col

Schema_map = {
    "customer_id": "int",
    "customer_name": "string",
    "gender": "string",
    "age": "int",
    "city": "string",
    "signup_date": "timestamp"
}

df = spark.read.table("workspace.dbo.transactioncustomermaster")


casted = df
for c, t in Schema_map.items():
    casted = casted.withColumn(c, col(c).cast(t))

display(casted)

# COMMAND ----------

Customer_Brozne_1 = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("signup_date", TimestampType(), True)
])

df_1 = spark.read.table("workspace.dbo.transactioncustomermaster")

casted_1 = df_1
for field in Customer_Brozne_1.fields:
    casted_1 = casted_1.withColumn(
        field.name,
        col(field.name).cast(field.dataType)
    )

display(casted_1)

# COMMAND ----------

# MAGIC %md
# MAGIC Window Spec (Window function in Pyspark)
# MAGIC -----------------------------------------
# MAGIC
# MAGIC 1. first we need to import the API called as Window
# MAGIC 2. Create the window 
# MAGIC 3. Use the data fames and then use the agg function based on the needs.
# MAGIC
# MAGIC

# COMMAND ----------

window_spec = Window.partitionBy("gender").orderBy("city")
df = casted_1.withColumn("Count",row_number().over(window_spec))
display(df)

# COMMAND ----------


