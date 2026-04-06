# Databricks notebook source
# MAGIC %run "/Workspace/Users/surajss.3110@gmail.com/Learning/PySpark/Transactions/Transactions-Master/CustomerMaster"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/surajss.3110@gmail.com/Learning/PySpark/Transactions/Transactions-Master/ProductMaster"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/surajss.3110@gmail.com/Learning/PySpark/Transactions/Transactions-Master/StoreMaster"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/surajss.3110@gmail.com/Learning/PySpark/Transactions/Transactions-Fact/PaymentFact"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/surajss.3110@gmail.com/Learning/PySpark/Transactions/Transactions-Fact/PromotionFact"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/surajss.3110@gmail.com/Learning/PySpark/Transactions/Transactions-Fact/ReturnsFact"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/surajss.3110@gmail.com/Learning/PySpark/Transactions/Transactions-Transaction/Transaction"

# COMMAND ----------

df_CustomerMaster= ReturnCustomerMasters()
df_ProductMaster = ReturnProductMaster()
df_StoreMaster = ReturnStoreMaster()
df_ReturnFact = ReturnPyamentFact()
df_PromotionFact = ReturnPromotion()
df_PaymentFact = ReturnPyamentFact()
df_TransactionsTransaction = ReturnTransaction()

# COMMAND ----------

df_CustomerMaster.write.format("delta").mode("overwrite").saveAsTable("workspace.dbo.TransactionCustomerMaster")
df_ProductMaster.write.format("delta").mode("overwrite").saveAsTable("workspace.dbo.TransactionProductMaster")
df_StoreMaster.write.format("delta").mode("overwrite").saveAsTable("workspace.dbo.TransactionStoresMaster")
df_ReturnFact.write.format("delta").mode("overwrite").saveAsTable("workspace.dbo.TransactionReturnFact")
df_PromotionFact.write.format("delta").mode("overwrite").saveAsTable("workspace.dbo.TransactionPromotionFact")
df_PaymentFact.write.format("delta").mode("overwrite").saveAsTable("workspace.dbo.TransactionPaymentFact")
df_TransactionsTransaction.write.format("delta").mode("overwrite").saveAsTable("workspace.dbo.df_TransactionsTransaction")
