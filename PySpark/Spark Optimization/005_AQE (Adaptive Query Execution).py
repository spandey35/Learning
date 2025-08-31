# Databricks notebook source
# MAGIC %md
# MAGIC # **_AQE (Adaptive Query Execution)_**

# COMMAND ----------

# MAGIC %md
# MAGIC **_What is Adaptive Query Execution (AQE)?_**
# MAGIC
# MAGIC Adaptive Query Execution (AQE) is a feature introduced in Apache Spark 3.0 that allows Spark to dynamically optimize query execution plans based on runtime statistics. 
# MAGIC Traditionally, Spark generates a static execution plan during the query analysis phase, which may not be optimal due to inaccurate or missing statistics.
# MAGIC
# MAGIC AQE enhances this by modifying the physical plan during execution, allowing Spark to make smarter decisions such as c**hanging join strategies**, **handling data skew**, and **coalescing 
# MAGIC shuffle partitions**. This leads to better performance, reduced resource usage, and more efficient execution of queries, especially in distributed environments.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **_Why is AQE Important?_**
# MAGIC
# MAGIC In real-world data processing, datasets are often unpredictable some partitions may be skewed, some joins may be inefficient, and some shuffle operations may create too many small tasks. Static query plans cannot adapt to these runtime realities.
# MAGIC
# MAGIC AQE solves this by:
# MAGIC
# MAGIC 1. **Handling Data Skew**
# MAGIC    - Automatically detects skewed partitions and applies optimizations like splitting skewed tasks or using broadcast joins to avoid bottlenecks.
# MAGIC
# MAGIC 2. **Dynamic Join Strategy Selection**
# MAGIC    - Switches join types (e.g., from sort-merge to broadcast) based on actual data size, improving performance and reducing shuffle overhead.
# MAGIC
# MAGIC 3. **Optimizing Partition Sizes**
# MAGIC    - Coalesces small shuffle partitions into fewer, larger ones to reduce task scheduling overhead and improve parallelism.
# MAGIC
# MAGIC 4. **Improved Query Performance**
# MAGIC    - Queries run faster because Spark adapts to the actual data characteristics instead of relying on estimates.
# MAGIC
# MAGIC 5. **Better Resource Utilization**
# MAGIC    - Reduces memory usage, CPU cycles, and network traffic by choosing optimal execution paths.
# MAGIC
# MAGIC 6. **Simplifies Tuning**
# MAGIC    - Reduces the need for manual tuning of Spark configurations, making it easier for developers to write efficient code.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

## truning  off  of the AQE
spark.conf.set("spark.sql.adaptive.enabled", "false")

# COMMAND ----------

## Loading the sales data from a CSV file 

df_sales = (spark.read
           .format("csv")
          .option("inferschema", True)
          .option("header", True)
          .load("/FileStore/Optimization/Salesdata.csv"))

# COMMAND ----------

## Checking Number of Partitions

df_sales.rdd.getNumPartitions()

# COMMAND ----------

display(df_sales)

# COMMAND ----------

## Performs aggregation to count the number of records per product category. This is a simple transformation to test performance with AQE off.

df = df_sales.groupBy("ProductCategory").count()
display(df)

# COMMAND ----------

## truning  On  of the AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------

## Reloads the same dataset to ensure a fresh execution context with AQE enabled.

df_sales_new = (spark.read
           .format("csv")
          .option("inferschema", True)
          .option("header", True)
          .load("/FileStore/Optimization/Salesdata.csv"))

# COMMAND ----------

## Repeats the aggregation to observe differences in execution plan and performance with AQE enabled.

df_new = df_sales_new.groupBy("ProductCategory").count()
display(df_new)

# COMMAND ----------

# MAGIC %md
# MAGIC **Best Practices with AQE**
# MAGIC
# MAGIC - Enable AQE in production unless specific issues arise.
# MAGIC - Use AQE with large or skewed datasets to benefit from dynamic optimizations.
# MAGIC - Monitor Spark UI to understand how AQE modifies execution plans.
# MAGIC - Combine AQE with other optimizations like caching, broadcast hints, and partitioning strategies.
# MAGIC - Test queries with and without AQE during development to understand its impact.
# MAGIC - Tune related configs:
# MAGIC   - `spark.sql.adaptive.skewJoin.enabled`
# MAGIC   - `spark.sql.adaptive.coalescePartitions.enabled`
# MAGIC   - `spark.sql.adaptive.localShuffleReader.enabled`

# COMMAND ----------

# MAGIC %md
# MAGIC **Comparison Table: With vs Without AQE**
# MAGIC
# MAGIC | Feature                        | Without AQE                  | With AQE                        |
# MAGIC |-------------------------------|------------------------------|---------------------------------|
# MAGIC | Join Strategy                 | Static                       | Dynamic (based on data size)   |
# MAGIC | Partition Coalescing          | Manual                       | Automatic                       |
# MAGIC | Skew Join Handling            | Not handled                  | Automatically optimized         |
# MAGIC | Performance                   | May degrade with skewed data | Improved with runtime tuning   |
# MAGIC | Resource Utilization          | Less efficient               | More efficient                  |
# MAGIC | Execution Plan                | Fixed                        | Adaptive                        |
# MAGIC | Developer Effort              | High (manual tuning)         | Lower (auto optimization)       |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Practice Tips**
# MAGIC
# MAGIC - Try AQE with different types of datasets:
# MAGIC   - Skewed vs balanced
# MAGIC   - Small vs large
# MAGIC   - Wide vs narrow schemas
# MAGIC
# MAGIC - Use Spark UI to analyze:
# MAGIC   - Physical plans
# MAGIC   - Shuffle stages
# MAGIC   - Task distribution
# MAGIC
# MAGIC - Experiment with AQE-related configurations:
# MAGIC   - `spark.sql.adaptive.enabled`
# MAGIC   - `spark.sql.adaptive.skewJoin.enabled`
# MAGIC   - `spark.sql.adaptive.coalescePartitions.enabled`
# MAGIC   - `spark.sql.adaptive.localShuffleReader.enabled`
# MAGIC
# MAGIC - Compare execution time and resource usage with AQE on and off.
# MAGIC
# MAGIC - Build test cases:
# MAGIC   - Join two datasets with skewed keys
# MAGIC   - Perform groupBy on unevenly distributed data
# MAGIC   - Analyze shuffle partition sizes
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Conclusion**
# MAGIC
# MAGIC Adaptive Query Execution (AQE) is a game-changing feature in Apache Spark that brings intelligence to query execution. By adapting plans based on runtime statistics, AQE helps Spark overcome challenges like data skew, inefficient joins, and suboptimal partitioning.
# MAGIC
# MAGIC For data engineers and analysts, understanding AQE is essential to building scalable, performant, and resource-efficient data pipelines. With AQE, Spark becomes more robust and capable of handling real-world data complexities with minimal manual intervention.
# MAGIC

# COMMAND ----------


