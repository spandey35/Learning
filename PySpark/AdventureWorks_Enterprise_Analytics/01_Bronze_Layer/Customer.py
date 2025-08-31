# Databricks notebook source
# Reading the data from table
df_sales_Customer = spark.read.table("workspace.sales.customer")

# Write to Bronze Layer in Delta format
df_sales_Customer.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/deltalackeandlakehouse/DeltaTables/AdventureWorks_Enterprise_Analytics/mnt/bronze/customer")


