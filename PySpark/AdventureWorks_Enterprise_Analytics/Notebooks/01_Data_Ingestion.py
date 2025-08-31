# Databricks notebook source
# read the data from source and Write to Bronze Layer in Delta format for customer

df_sales_customer = spark.read.table("workspace.sales.customer")

df_sales_customer.write
    .format("delta")
    .mode("overwrite")
    .save("/Volumes/workspace/default/adventureworks2022/DeltaTables/Ingestion/Customer")



# COMMAND ----------

# read the data from source and Write to Bronze Layer in Delta format for sales_order_header

df_sales_order_header = spark.read.table("workspace.sales.sales_order_header")

df_sales_order_header
    .write.format("delta")
    .mode("overwrite")
    .save("/Volumes/workspace/default/adventureworks2022/DeltaTables/Ingestion/sales_order_header")

    

# COMMAND ----------

# read the data from source and Write to Bronze Layer in Delta format for sales_order_detail

df_sales_order_detail = spark.read.table("workspace.sales.sales_order_detail")

df_sales_order_detail
    .write.format("delta")
    .mode("overwrite")
    .save("/Volumes/workspace/default/adventureworks2022/DeltaTables/Ingestion/sales_order_detail")

    

# COMMAND ----------

# read the data from source and Write to Bronze Layer in Delta format for Product

df_sales_product = spark.read.table("workspace.production.product")

df_sales_product
    .write
    .format("delta")
    .mode("overwrite").save("/Volumes/workspace/default/adventureworks2022/DeltaTables/Ingestion/product")


