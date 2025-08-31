# Databricks notebook source
# Step 1: Read raw data from AdventureWorks table
df_sales_header = spark.read.table("workspace.sales.Sales_Order_Header")

# Step 2: Optional - Basic schema and data checks
df_sales_header.printSchema()
df_sales_header.show(5)

# Step 3: Write to Bronze Layer in Delta format
df_sales_header.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/deltalackeandlakehouse/DeltaTables/AdventureWorks_Enterprise_Analytics/mnt/bronze/SalesOrderHeader")


