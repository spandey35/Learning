# Databricks notebook source
# MAGIC %md
# MAGIC # **_Caching and Persistence_**
# MAGIC
# MAGIC ## 1. What is Caching and Persistence?
# MAGIC - **Caching** and **Persistence** are mechanisms in Spark to store intermediate DataFrames/RDDs in memory (and optionally disk) to avoid recomputation.  
# MAGIC - **Caching** is a shorthand for `.persist(StorageLevel.MEMORY_ONLY)`.  
# MAGIC - **Persistence** provides multiple storage level options such as memory, disk, serialized, or off-heap.  
# MAGIC - Useful when the same DataFrame/RDD is accessed multiple times in different actions.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. How Does It Work Internally?
# MAGIC - When `.cache()` or `.persist()` is called, Spark marks the DataFrame/RDD to be stored in memory/disk after the first action triggers computation.  
# MAGIC - On the first action (e.g., `.count()`), Spark computes the dataset and stores partitions in the specified storage level.  
# MAGIC - On subsequent actions, Spark reuses cached partitions instead of recomputing the entire lineage.  
# MAGIC - If memory is insufficient:  
# MAGIC   - `cache()` (`MEMORY_ONLY`) → recomputes missing partitions.  
# MAGIC   - `persist(StorageLevel.MEMORY_AND_DISK)` → spills partitions to disk.  
# MAGIC - Data can be explicitly removed using `.unpersist()`.  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Benefits of Caching and Persistence
# MAGIC - Reduces recomputation overhead for iterative algorithms (e.g., ML, Graph Processing).  
# MAGIC - Speeds up interactive queries (SQL/DataFrames).  
# MAGIC - Improves performance in scenarios where the same dataset is reused multiple times.  
# MAGIC - Reduces execution latency for multiple actions on the same dataset.  
# MAGIC
# MAGIC ## 4. Limitations of Caching and Persistence
# MAGIC - Consumes cluster memory (can cause GC overhead or OOM).  
# MAGIC - Not always beneficial for one-time computations.  
# MAGIC - Requires manual management (`.unpersist()`) to free up memory.  
# MAGIC - Wrong storage level selection may degrade performance.  

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Performance Impact
# MAGIC - Huge speedup when datasets are reused frequently.  
# MAGIC - Saves CPU cycles by avoiding recomputation of entire DAG lineage.  
# MAGIC - Reduces shuffle overhead in iterative workloads.  
# MAGIC - However, improper use may cause memory pressure and job slowdown.  

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Best Practices
# MAGIC - Use `.cache()` for DataFrames reused multiple times in a single job.  
# MAGIC - Use `.persist()` with proper `StorageLevel` for large datasets.  
# MAGIC - Call `.unpersist()` after the cached data is no longer needed.  
# MAGIC - Monitor Spark UI → *Storage Tab* for cached RDDs/DataFrames.  
# MAGIC - Avoid caching very large datasets that don’t fit in memory.  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Syntax
# MAGIC
# MAGIC
# MAGIC Cache DataFrame (memory only)  --->  df.cache()
# MAGIC
# MAGIC **Persist DataFrame with memory and disk**
# MAGIC
# MAGIC - from pyspark import StorageLevel
# MAGIC - df.persist(StorageLevel.MEMORY_AND_DISK)
# MAGIC
# MAGIC Remove from cache ---> df.unpersist()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

## Real-World PySpark Example and reading the files from DBFS. 

df_sales = (spark.read
           .format("csv")
          .option("inferschema", True)
          .option("header", True)
          .load("/FileStore/Optimization/Salesdata.csv"))

display(df_sales)          


# COMMAND ----------

df1 = df_sales.filter(col("ProductCategory") == 'Foods')

# COMMAND ----------

df2 = df_sales.filter(col("ProductCategory") == 'Electronics')

# COMMAND ----------

# MAGIC %md
# MAGIC In the above example, the `df_sales` DataFrame is being recomputed multiple times whenever an action is executed.  
# MAGIC This repeated computation increases execution time and slows down performance.  
# MAGIC To overcome this issue, we can use **`cache()`** or **`persist()`** to store the DataFrame in memory (and optionally disk),  
# MAGIC so that subsequent actions can reuse the cached results instead of recalculating the entire lineage.
# MAGIC

# COMMAND ----------


