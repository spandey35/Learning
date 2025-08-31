# Databricks notebook source
# MAGIC %md
# MAGIC # **SQL Hints**
# MAGIC
# MAGIC SQL Hints are optional directives embedded in SQL queries that influence the behavior of Spark’s query optimizer. 
# MAGIC While Spark’s Catalyst optimizer is powerful and usually makes good decisions automatically, there are cases where manual guidance can lead to better performance—especially when you know our data well.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Why Use SQL Hints?**
# MAGIC
# MAGIC Spark’s optimizer uses statistics and heuristics to choose execution plans. However, in real-world scenarios:
# MAGIC
# MAGIC - Data may be skewed.
# MAGIC - Size estimates may be inaccurate.
# MAGIC - Join strategies may not be optimal.
# MAGIC - You may want to force a specific behavior (e.g., broadcast a table).
# MAGIC
# MAGIC SQL hints allow you to override default decisions and guide Spark toward more efficient execution.

# COMMAND ----------

# MAGIC %md
# MAGIC **Common SQL Hints in Spark**
# MAGIC
# MAGIC | **Hint Name**             | **Description**                                                   | **Syntax Example**                                                   |
# MAGIC |---------------------------|-------------------------------------------------------------------|----------------------------------------------------------------------|
# MAGIC | `BROADCAST`               | Forces Spark to broadcast the specified table in a join.          | `SELECT /*+ BROADCAST(table_alias) */ ...`                           |
# MAGIC | `MERGE`                   | Suggests using a Sort Merge Join strategy.                        | `SELECT /*+ MERGE(table_alias) */ ...`                               |
# MAGIC | `SHUFFLE_HASH`            | Suggests using a Shuffle Hash Join strategy.                      | `SELECT /*+ SHUFFLE_HASH(table_alias) */ ...`                        |
# MAGIC | `SHUFFLE_REPLICATE_NL`    | Suggests using Replicated Nested Loop Join.                       | `SELECT /*+ SHUFFLE_REPLICATE_NL(table1, table2) */ ...`             |
# MAGIC | `COALESCE`                | Reduces the number of partitions in the output.                   | `SELECT /*+ COALESCE(4) */ ...`                                      |
# MAGIC | `REPARTITION`             | Increases or changes the number of partitions.                    | `SELECT /*+ REPARTITION(8) */ ...`                                   |
# MAGIC | `REBALANCE`               | Redistributes data evenly across partitions.                      | `SELECT /*+ REBALANCE */ ...`                                        |
# MAGIC | `MAPJOIN`                 | Alias for BROADCAST.                                              | `SELECT /*+ MAPJOIN(table_alias) */ ...`                             |
# MAGIC | `JOIN`                    | Specifies join strategy and order.                                | `SELECT /*+ JOIN(table1, table2, strategy) */ ...`                   |
# MAGIC | `SKEW`                    | Helps handle data skew in joins.                                  | `SELECT /*+ SKEW(table_alias) */ ...`                                |
# MAGIC

# COMMAND ----------

## Real-World PySpark Example and reading the files from DBFS. 

from pyspark.sql.functions import *

df_sales = (spark.read
           .format("csv")
          .option("inferschema", True)
          .option("header", True)
          .load("/FileStore/Optimization/Salesdata.csv"))

df_dim_product = (spark.read
                  .format("csv")
                  .option("inferschema", True)
                  .option("header", True)
                  .load("/FileStore/Optimization/dim_product.csv"))

df_dim_Customer = (spark.read
                   .format("csv")
                   .option("inferschema", True)
                   .option("header", True)
                   .load("/FileStore/Optimization/dim_customer.csv"))







# COMMAND ----------

##  Register DataFrames as Temp Views

df_sales.createOrReplaceTempView("df_sales")
df_dim_product.createOrReplaceTempView("df_dim_product")
df_dim_Customer.createOrReplaceTempView("df_dim_Customer")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT /*+ BROADCAST(df_dim_product), MERGE(df_dim_Customer) */
# MAGIC        s.Product, p.ProductName, c.CustomerName
# MAGIC FROM df_sales s
# MAGIC JOIN df_dim_product p ON s.Product = p.ProductID
# MAGIC JOIN df_dim_Customer c ON s.Customer = c.CustomerID
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Impact of SQL Hints
# MAGIC
# MAGIC | Feature              | Without Hints         | With Hints                          |
# MAGIC |----------------------|-----------------------|-------------------------------------|
# MAGIC | Join Strategy        | Auto-selected         | Manually controlled                 |
# MAGIC | Broadcast Behavior   | Based on size estimate| Explicitly forced                   |
# MAGIC | Partition Handling   | Default               | Tuned via `COALESCE` / `REPARTITION`|
# MAGIC | Skew Handling        | Reactive              | Proactive via `SKEW` hint           |
# MAGIC | Execution Time       | Variable              | Often improved                      |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##  Best Practices for Using SQL Hints
# MAGIC
# MAGIC - **Know Your Data**: Use hints only when you understand the size, distribution, and characteristics of your datasets.
# MAGIC - **Validate with `EXPLAIN`**: Always check the physical plan to ensure your hint was applied.
# MAGIC - **Avoid Overuse**: Too many hints can lead to confusing or suboptimal plans.
# MAGIC - **Tune Broadcast Threshold**: Use `spark.sql.autoBroadcastJoinThreshold` to control automatic broadcasting.
# MAGIC - **Use Spark UI**: Monitor stages and tasks to see the impact of hints.
# MAGIC
