# Databricks notebook source
# Reading the data from table
df_sales_order_details = spark.read.table("workspace.sales.sales_order_detail")

# Write to Bronze Layer in Delta format
df_sales_order_details.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/deltalackeandlakehouse/DeltaTables/AdventureWorks_Enterprise_Analytics/mnt/bronze/SalesOrderDetails")

