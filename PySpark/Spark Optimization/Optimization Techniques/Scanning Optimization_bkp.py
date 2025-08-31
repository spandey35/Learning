# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.get("spark.sql.adaptive.enabled",)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading files the from DBFS**

# COMMAND ----------

df_sales = (spark.read
            .format("csv")
            .option("inferschema", True)
            .option("header", True)
            .load("/FileStore/Optimization/Salesdata.csv"))

df_dim_product = (spark.read
                 .format("csv")
                .option("inferschema", True)
                .option("header", True)
                .load("/FileStore/Optimization/dim_product.csv"))

df_dim_Customer = (spark.read
                 .format("csv")
                 .option("inferschema", True)
                 .option("header", True)
                .load("/FileStore/Optimization/dim_customer.csv"))


# COMMAND ----------

display(df_sales)

# COMMAND ----------

df_sales.rdd.getNumPartitions()

# COMMAND ----------

# Changing the default partition size to 128kb each. 
spark.conf.set("spark.sql.files.maxParitionBytes", 134217728) 

# Changing the default partition size to 128mb each
spark.conf.set("spark.sql.files.maxParitionBytes", 134217728) 

# COMMAND ----------

df_sales.rdd.getNumPartitions()

# COMMAND ----------

# Repartitioning

df_sales.repartition(20)

# COMMAND ----------

df_sales.rdd.getNumPartitions()

# COMMAND ----------

df_sales.withColumn("Parition_id", spark_partition_id()).display()
