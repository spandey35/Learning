# Databricks notebook source
# MAGIC %run /Workspace/Users/surajss.3110@gmail.com/Learning/PySpark/AdventureWorks_Enterprise_Analytics/Notebooks/02_Data_Cleaning

# COMMAND ----------

cust = load_customer()
prod = load_product()
orderhead = load_sales_order_header()
orderdetails = load_sales_order_detail()

# COMMAND ----------

# Estimate how valuable each customer is based on their purchase history.

CustomerLifetimeValue = (orderhead
                         .join(orderdetails, orderhead.SalesOrderID == orderdetails.SalesOrderID)
                         .groupBy("CustomerID").agg(
                            round(avg("SubTotal"),2).alias("Avg SubTotal"),
                            count(orderdetails.SalesOrderID).alias("Frequency")
                         )
                         )

display(CustomerLifetimeValue)                         

# COMMAND ----------

from pyspark.sql.functions import avg, count, sum, max, min, datediff, current_date, round

CustomerLifetimeValue = (
    orderhead
    .join(orderdetails, "SalesOrderID")
    .groupBy("CustomerID")
    .agg(
        round(avg("SubTotal"), 2).alias("AvgOrderValue"),
        count("SalesOrderID").alias("Frequency"),
        round(sum("SubTotal"), 2).alias("TotalRevenue"),
        max("OrderDate").alias("LastPurchaseDate"),
        min("OrderDate").alias("FirstPurchaseDate")
    )
    .withColumn("Recency", datediff(current_date(), col("LastPurchaseDate")))
    .withColumn("CustomerLifespan", datediff(col("LastPurchaseDate"), col("FirstPurchaseDate")))
    .withColumn("CLTV", round(col("AvgOrderValue") * col("Frequency") * (col("CustomerLifespan") / 30), 2))  # Assuming lifespan in months
)

display(CustomerLifetimeValue)

