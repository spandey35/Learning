# Databricks notebook source
# MAGIC %run /Workspace/Users/surajss.3110@gmail.com/Learning/PySpark/MasterNoteBooks/Sales_MasterNoteBook

# COMMAND ----------

tran = load_trans()
prod = load_product()
cust =load_customer()


# COMMAND ----------

df = tran.where(
    (col("payment_method") == "Credit Card") & 
    (col("price") > 500)
)

display(df)
  
