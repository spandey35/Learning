# Databricks notebook source
from pyspark.sql.window import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Reading the data from default Volume
df_customer_raw = spark.read.csv(path="/Volumes/workspace/default/behavior_analytics_e-commerce/Customer.csv",
                                 header=True,
                                 inferSchema=True)

df_product_raw = spark.read.csv(path="/Volumes/workspace/default/behavior_analytics_e-commerce/products.csv",
                                header = True,
                                inferSchema= True )

df_purchase_raw = spark.read.csv(path="/Volumes/workspace/default/behavior_analytics_e-commerce/purchases.csv",
                                 header = True,
                                 inferSchema= True)

df_sessions_raw = spark.read.csv(path="/Volumes/workspace/default/behavior_analytics_e-commerce/sessions.csv",
                                 header=True,
                                 inferSchema= True)

df_customer_raw.show(2)
df_product_raw.show(2)
df_purchase_raw.show(2)
df_sessions_raw.show(2)

# COMMAND ----------

# DBTITLE 1,Data Modeling & Enrichment
# 1. Build an enriched fact table combining customer, product, purchase, and session data such that:
#      Each record represents one purchase
#      Includes customer demographics, product metadata, and derived session metrics
#      Handles missing sessions gracefully


df_all_details  = (df_purchase_raw.
                   join(df_product_raw, "product_id" , "inner")
                   .join(df_customer_raw, "customer_id", "inner")
                   .join(df_sessions_raw, "customer_id", "inner")
                   .select("customer_id",
                           df_customer_raw.name,
                           df_customer_raw.email,
                           df_customer_raw.gender,
                           df_customer_raw.age,
                           df_customer_raw.location,
                           "product_id",
                           df_product_raw.brand,
                           df_product_raw.category,
                           df_product_raw.price,
                           df_product_raw.rating,
                           "transaction_id",
                           "purchase_date",
                           "purchase_delivery",
                           "quantity",
                           "total_amount")
)

display(df_all_details)
df_all_details.printSchema()

# COMMAND ----------

# DBTITLE 1,Advanced Aggregations & Revenue Analytics
## Calculate total revenue, total quantity sold, and average order value (AOV) per product category per month.

df_revenue = (
    df_all_details
    .withColumn("Year", year("purchase_date"))
    .withColumn("Month_Name", monthname("purchase_date"))
    .groupBy("category", "Year", "Month_Name")
    .agg(
        round(sum("total_amount"),2).alias("Total_Revenue"),
        round(sum("quantity"),2).alias("Total_Quantitu_Sold"),
        round(avg("Price"),2).alias("Avarage_Price")
    )
)

display(df_revenue)
df_revenue.printSchema()

# COMMAND ----------

## Find customers contributing to the top 20% of total revenue (Pareto analysis)

