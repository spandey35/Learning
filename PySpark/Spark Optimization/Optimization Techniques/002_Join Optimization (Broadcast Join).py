# Databricks notebook source
# MAGIC %md
# MAGIC # **Broadcast Join**
# MAGIC
# MAGIC **What is a Broadcast Join?**
# MAGIC
# MAGIC A **Broadcast Join** is a specialized join strategy in Apache Spark that is used when one of the datasets involved in the join is **small enough to fit into the memory of each executor node**. Instead of shuffling both datasets across the cluster, Spark **broadcasts the smaller dataset to all nodes**. This allows each node to perform the join locally, significantly reducing network traffic and improving performance.
# MAGIC
# MAGIC Broadcast joins are particularly useful in **star schema** designs, where a large fact table is joined with smaller dimension tables.

# COMMAND ----------

# MAGIC %md
# MAGIC **How Does It Work Internally?**
# MAGIC
# MAGIC - Size Estimation: Spark estimates the size of each dataset during query planning.
# MAGIC - Threshold Check: If the smaller dataset is below the broadcast threshold (default is 10MB, configurable via spark.sql.autoBroadcastJoinThreshold), Spark considers it for broadcasting.
# MAGIC - Broadcasting: The smaller dataset is serialized and sent to all executor nodes.
# MAGIC - Local Join Execution: Each executor performs the join between its partition of the large dataset and the broadcasted small dataset without shuffling.

# COMMAND ----------

# MAGIC %md
# MAGIC **Benefits of Broadcast Join**
# MAGIC
# MAGIC - No Shuffle Required: Avoids expensive data movement across the network.
# MAGIC - Faster Execution: Joins are performed locally, reducing latency.
# MAGIC - Efficient for Small Tables: Ideal when joining a large dataset with a small lookup or dimension table.
# MAGIC - Improves Scalability: Reduces pressure on the cluster during large joins.
# MAGIC
# MAGIC
# MAGIC **Limitations of Broadcast Join**
# MAGIC - Memory Usage: The small dataset must fit in memory on every executor.
# MAGIC - Manual Control Needed Sometimes: Spark may not automatically broadcast if the size estimation is inaccurate, requiring manual use of broadcast().

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

## First we will disbale the AQE

spark.conf.set("Spark.sql.adaptive.enabled", "false")
spark.conf.get("Spark.sql.adaptive.enabled")


# COMMAND ----------

## Real-World PySpark Example and reading the files from DBFS. 

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

## Without Broadcast Join

df_join = (df_sales
           .join(df_dim_Customer, df_sales["Customer"] == df_dim_Customer["CustomerId"])
           .join(df_dim_product, df_sales["Product"] == df_dim_product["ProductID"] ))

display(df_join)           

# COMMAND ----------

# MAGIC %md
# MAGIC **What Happens:**
# MAGIC - Spark performs a shuffle join.
# MAGIC - Both datasets are partitioned and shuffled across the cluster.
# MAGIC - This leads to high network I/O, especially if sales_df is huge.
# MAGIC - Execution time increases due to data movement.

# COMMAND ----------

## With Broadcast Join

df_broadcast_join = (
    df_sales
    .join(broadcast(df_dim_product), df_sales["Product"] == df_dim_product["ProductID"], "inner")
    .join(broadcast(df_dim_Customer), df_sales["Customer"] == df_dim_Customer["CustomerId"], "inner")
)

display(df_broadcast_join)
                  

# COMMAND ----------

# MAGIC %md
# MAGIC **What Happens:**
# MAGIC - df_customer and df_product is broadcasted to all executor nodes.
# MAGIC - Each node performs the join locally with its partition of sales_df.
# MAGIC - No shuffle is required.
# MAGIC - Execution is faster, and resource usage is optimized.

# COMMAND ----------

# MAGIC %md
# MAGIC **_Broadcast Join Comparison Table_**
# MAGIC
# MAGIC | Feature                  | Without Broadcast Join | With Broadcast Join |
# MAGIC |--------------------------|------------------------|----------------------|
# MAGIC | Shuffle Required         | Yes                 |  No                |
# MAGIC | Network Traffic          | High                |  Low               |
# MAGIC | Execution Time           |  Slower              | Faster            |
# MAGIC | Memory Usage             |  Balanced            | Higher on nodes   |
# MAGIC | Best Use Case            | Large-Large Joins      | Large-Small Joins    |
# MAGIC | Manual Control Available | No                  | Yes (`broadcast()`) |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Notes for Practice**
# MAGIC - Always check the size of the smaller dataset before deciding to broadcast.
# MAGIC - Use broadcast() explicitly when Spark doesn’t auto-broadcast due to inaccurate size estimation.
# MAGIC - Monitor memory usage when broadcasting large datasets manually.
# MAGIC - Tune spark.sql.autoBroadcastJoinThreshold if needed for larger dimension tables.
