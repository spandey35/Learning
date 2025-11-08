# Databricks notebook source
# MAGIC %md
# MAGIC **Purpose of this notebook**
# MAGIC - Lean about the basic behavior of Delta Lake
# MAGIC - Understanding _deltal_log structure and meaning
# MAGIC - Understanding importance of file skipping

# COMMAND ----------

# MAGIC %md
# MAGIC **Setup**

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "false")

