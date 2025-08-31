# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema dbo

# COMMAND ----------

# MAGIC %sql
# MAGIC create table dbo.Sales
# MAGIC (id  int,
# MAGIC salary int)
# MAGIC
# MAGIC using delta location "/FileStore/deltaTable"

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dbo.sales values(1, 100)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dbo.sales values(2, 400);
# MAGIC insert into dbo.sales values(10, 100)

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize dbo.sales zorder By(id)

# COMMAND ----------


