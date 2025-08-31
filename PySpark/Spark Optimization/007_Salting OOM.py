# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC # **_Salting in Spark_**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **What is Salting in Spark?**
# MAGIC
# MAGIC **Salting** is a technique used in Apache Spark to address the problem of **data skew** a common issue in distributed computing where certain keys in a dataset have disproportionately large amounts of data compared to others.
# MAGIC
# MAGIC In Spark, operations like joins and aggregations rely on evenly distributed data across partitions. When one key (e.g., `'India'`) has millions of records and others have only a few, Spark tasks become unbalanced. This leads to performance degradation, long-running tasks, and even **Out of Memory (OOM)** errors.
# MAGIC
# MAGIC Salting works by artificially modifying the skewed key to distribute its data across multiple partitions, thereby balancing the workload and improving performance.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Why is Salting Needed in Spark?**
# MAGIC
# MAGIC Salting is necessary when Spark encounters **data skew** during operations like joins, groupBy, or aggregations. Data skew causes:
# MAGIC
# MAGIC - **Unbalanced task execution**: Some tasks take much longer than others.
# MAGIC - **OOM errors**: Tasks handling skewed keys may exceed memory limits.
# MAGIC - **Cluster inefficiency**: Resources are underutilized due to bottlenecks.
# MAGIC - **Poor scalability**: Queries fail to scale with larger datasets.
# MAGIC
# MAGIC Salting helps mitigate these issues by **distributing the skewed key’s data across multiple partitions**, allowing Spark to parallelize the workload more effectively.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Understanding Salting in Spark**
# MAGIC
# MAGIC  **How Salting Works:**
# MAGIC
# MAGIC 1. **Identify the skewed key**: Analyze the data distribution to find keys with excessive records.
# MAGIC 2. **Add a salt value**: Append a random number or hash to the skewed key in the large (fact) table.
# MAGIC 3. **Replicate the key**: Duplicate the matching key in the small (dimension) table for each salt value.
# MAGIC 4. **Join on salted keys**: Perform the join using the modified keys to distribute the load.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Conclusion: Salting in Spark**
# MAGIC
# MAGIC **Key Observations**:
# MAGIC
# MAGIC - Salting is a **manual optimization technique** used to handle skewed data in Spark.
# MAGIC - It is most effective when dealing with **large fact tables** joined with **small dimension tables**.
# MAGIC - Salting introduces **additional logic** in data preparation and join conditions but significantly improves performance.
# MAGIC
# MAGIC **Benefits of Salting in Spark:**
# MAGIC
# MAGIC - Prevents **OOM errors** caused by skewed partitions.
# MAGIC - Improves **parallelism** and **task distribution**.
# MAGIC - Reduces **execution time** for skewed joins.
# MAGIC - Enhances **cluster resource utilization**.
# MAGIC - Makes Spark jobs more **scalable and stable**.
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Final Takeaway:**
# MAGIC
# MAGIC Salting is a powerful and practical solution to overcome data skew in Spark. While it adds complexity to the join logic, the benefits in terms of performance and reliability are substantial. It is especially useful in **ETL pipelines**, **data warehousing**, and **analytics workloads** where skewed keys are common.
# MAGIC
# MAGIC Use salting when you observe:
# MAGIC - Uneven task durations
# MAGIC - Long-running stages
# MAGIC - Frequent OOM errors
# MAGIC - Poor scalability in joins or aggregations
# MAGIC
# MAGIC By applying salting strategically, you can ensure that Spark jobs run efficiently and reliably across large datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC **Salting Example**
# MAGIC
# MAGIC 1. Assume that for `User_id = 'A'`, we have more than **70% of the total records** in the dataset. This creates a **data skew** problem.
# MAGIC 2. The DataFrame is **partitioned based on `User_id`**, which means most of the data will go into a single partition for `'A'`.
# MAGIC 3. Now, we want to **join this skewed DataFrame** with another small DataFrame (e.g., a lookup table) on `User_id`.
# MAGIC
# MAGIC This setup will likely cause **performance issues** or **OOM errors** due to the skewed distribution. To solve this, we will apply **salting** to distribute the skewed key across multiple partitions.
# MAGIC

# COMMAND ----------

# Sample skewed dataset
data = [("A", 100), ("A", 200), ("A", 300), ("B", 400), ("C", 500), ("C", 100)]
df = spark.createDataFrame(data, ["User_id", "Sales"])

display(df)

# COMMAND ----------

# Small lookup DataFrame with user details
lookup_data = [("A", "Gold"), ("B", "Silver"), ("C", "Bronze")]
df_lookup = spark.createDataFrame(lookup_data, ["User_id", "Membership"])

display(df_lookup)


# COMMAND ----------

# Add salt to skewed key in main DataFrame
df_salted = df.withColumn("salt", (rand() * 10).cast("int"))
df_salted = df_salted.withColumn("salted_key", concat_ws("_", col("User_id"), col("salt")))

display(df_salted)

# COMMAND ----------

# Create salt values to match the salted keys
salt_range = list(range(10))
df_salt_values = spark.createDataFrame(salt_range, "int").withColumnRenamed("value", "salt")

# Cross join to replicate lookup keys for each salt value
df_lookup_expanded = df_lookup.crossJoin(df_salt_values)
df_lookup_expanded = df_lookup_expanded.withColumn("salted_key", concat_ws("_", col("User_id"), col("salt")))

display(df_lookup_expanded)


# COMMAND ----------

# Join on salted_key to distribute skewed data
df_joined = df_salted.join(df_lookup_expanded, on="salted_key", how="inner")

display(df_joined)

