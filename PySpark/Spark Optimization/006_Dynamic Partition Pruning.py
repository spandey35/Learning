# Databricks notebook source
# MAGIC %md
# MAGIC # **_Dynamic Partition Pruning_**

# COMMAND ----------

# MAGIC %md
# MAGIC **What is Dynamic Partition Pruning (DPP)?**
# MAGIC
# MAGIC Dynamic Partition Pruning (DPP) is a runtime optimization feature in Apache Spark that improves query performance when filtering on partitioned columns during joins.
# MAGIC
# MAGIC In traditional static partition pruning, Spark can only prune partitions if the filter value is known at compile time. However, in many real-world scenarios, the filter value is only available during query execution especially in joins. DPP solves this by dynamically determining which partitions to read based on the join key values observed at runtime.
# MAGIC
# MAGIC This significantly reduces the amount of data read from disk, improves I/O efficiency, and speeds up query execution.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Why is DPP Important?**
# MAGIC
# MAGIC 1. **Efficient Data Scanning**
# MAGIC    - Avoids scanning unnecessary partitions by pruning them dynamically based on join keys.
# MAGIC
# MAGIC 2. **Improved Query Performance**
# MAGIC    - Reduces I/O and memory usage, leading to faster execution times.
# MAGIC
# MAGIC 3. **Works with Runtime Filters**
# MAGIC    - Enables partition pruning even when filter values are not known until the query runs.
# MAGIC
# MAGIC 4. **Ideal for Star Schema Joins**
# MAGIC    - Especially useful in data warehousing scenarios where a large fact table is joined with smaller dimension tables.
# MAGIC
# MAGIC 5. **Reduces Shuffle and Network Overhead**
# MAGIC    - By reading only relevant partitions, Spark minimizes data movement across the cluster.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled","false")

# COMMAND ----------

spark.conf.get("spark.sql.adaptive.enabled","false")

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

# COMMAND ----------

spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# COMMAND ----------

spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled","false")

# COMMAND ----------

spark.conf.get("spark.sql.optimizer.dynamicPartitionPruning.enabled")

# COMMAND ----------

# Loading the sales data from a CSV file 

df_sales = (spark.read
           .format("csv")
          .option("inferschema", True)
          .option("header", True)
          .load("/FileStore/Optimization/Salesdata.csv"))
df_sales.limit(50).display()          

# COMMAND ----------

# MAGIC %md
# MAGIC **Understanding Dynamic Partition Pruning in Spark**
# MAGIC
# MAGIC We will create two DataFrames to demonstrate how **Dynamic Partition Pruning (DPP)** works in Apache Spark, and observe the performance difference when **Adaptive Query Execution (AQE)** is turned **off** and **on**.
# MAGIC
# MAGIC **Step-by-Step Plan:**
# MAGIC
# MAGIC 1. **Create DataFrame 1 (`df1`)**:
# MAGIC    - This DataFrame will be **partitioned by `ProductCategory`**.
# MAGIC    - It simulates a large fact table where partitioning helps optimize query performance.
# MAGIC
# MAGIC 2. **Create DataFrame 2 (`df2`)**:
# MAGIC    - This DataFrame will **not be partitioned**.
# MAGIC    - It represents a smaller dimension table or a filter condition source.
# MAGIC
# MAGIC 3. **Join the DataFrames**:
# MAGIC    - Perform a join between `df1` and `df2` on the `ProductCategory` column.
# MAGIC    - First, execute the join with **AQE turned off**.
# MAGIC    - Then, execute the same join with **AQE turned on**.
# MAGIC
# MAGIC 4. **Compare the Results**:
# MAGIC    - Observe the **query execution plan** and **performance metrics**.
# MAGIC    - With AQE enabled, Spark should apply **Dynamic Partition Pruning**, meaning it will only scan the relevant partitions of `df1` based on the values in `df2`.
# MAGIC
# MAGIC This experiment will help us understand how Spark optimizes queries using **Dynamic Partition Pruning** and how enabling **AQE** can significantly improve performance in partitioned data scenarios.
# MAGIC

# COMMAND ----------

## Reading the data to load and will partition it based on ProductCategory
df_Source = df_sales.limit(500)


## Partitioning the datafame 
df1 = df_Source.write.partitionBy("ProductCategory").mode("overwrite").parquet("/FileStore/Optimizationpartitioned_df1")
df1 = spark.read.parquet("/FileStore/Optimizationpartitioned_df1")

## Loading all the datafames.
df2 = df_sales.select("ProductCategory").distinct()



# COMMAND ----------

## Checking the AQE is turned off or not
spark.conf.get("spark.sql.adaptive.enabled","false")

# COMMAND ----------

df_WithoutAQE = df1.join(
    df2.filter(col("ProductCategory") == "Food"),
    on="ProductCategory"
)

display(df_WithoutAQE)

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled","true")
spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------

df_WithAQE = df1.join(
    df2.filter(col("ProductCategory") == "Food"),
    on="ProductCategory"
)

display(df_WithoutAQE)

# COMMAND ----------


df_WithoutAQE.explain(True)
df_WithAQE.explain(True)


# COMMAND ----------

# MAGIC %md
# MAGIC **Conclusion: Dynamic Partition Pruning in Spark**
# MAGIC
# MAGIC Through this experiment, we demonstrated how **Dynamic Partition Pruning (DPP)** works in Apache Spark and how enabling **Adaptive Query Execution (AQE)** can significantly improve query performance.
# MAGIC
# MAGIC **Key Observations:**
# MAGIC
# MAGIC - When **AQE is turned off**, Spark scans **all partitions** of the fact table (`df1`), even though the filter condition (`ProductCategory = 'Food'`) is applied via the join.
# MAGIC - When **AQE is turned on**, Spark applies **Dynamic Partition Pruning**, scanning **only the relevant partitions** of `df1` based on the values in `df2`. This leads to:
# MAGIC   - Reduced I/O
# MAGIC   - Faster query execution
# MAGIC   - Lower memory and CPU usage
# MAGIC
# MAGIC **Benefits of DPP:**
# MAGIC
# MAGIC - Efficient data scanning by avoiding unnecessary partitions.
# MAGIC - Improved performance in star schema joins.
# MAGIC - Reduced shuffle and network overhead.
# MAGIC - Ideal for large-scale data warehousing scenarios.
# MAGIC
# MAGIC **Final Takeaway:**
# MAGIC
# MAGIC **Dynamic Partition Pruning** is a powerful optimization technique that becomes effective when **AQE is enabled**. It is especially beneficial in scenarios involving joins between large partitioned fact tables and smaller dimension tables. Always consider enabling AQE in your Spark environment to leverage this optimization for better performance and resource utilization.
# MAGIC
