# Databricks notebook source
# MAGIC %run /Workspace/Users/surajss.3110@gmail.com/Learning/PySpark/MasterNoteBooks/INC_MasterNoteBook

# COMMAND ----------

from pyspark.sql.functions import expr, rand, round, col
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.getOrCreate()

# Number of synthetic tickets
num_tickets = 1000

# Create base DataFrame
df = spark.range(0, num_tickets).withColumnRenamed("id", "Ticket_ID")

# Add synthetic fields
df = (df
      .withColumn("Priority", expr("CASE WHEN rand() < 0.6 THEN 'Low' WHEN rand() < 0.9 THEN 'Medium' ELSE 'High' END"))
      .withColumn("Assignment_Group", expr("CASE WHEN rand() < 0.3 THEN 'Network' WHEN rand() < 0.6 THEN 'Application' ELSE 'Support' END"))
      .withColumn("Opened", expr("current_timestamp() - make_interval(0, 0, 0, 0, int(rand() * 1000), 0)"))
      .withColumn("Resolution_Hours", round(rand() * 72).cast(IntegerType()))
      .withColumn("Resolved", expr("Opened + make_interval(0, 0, 0, 0, Resolution_Hours, 0)"))
      .withColumn("Issue_Type", expr("CASE WHEN rand() < 0.5 THEN 'Access' WHEN rand() < 0.8 THEN 'Outage' ELSE 'Performance' END"))
      .withColumn("SLA_Breached", expr("CASE WHEN Resolution_Hours > 48 THEN true ELSE false END"))
     )

# Display synthetic data
display(df)

