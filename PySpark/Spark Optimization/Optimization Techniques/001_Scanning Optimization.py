# Databricks notebook source
# MAGIC %md
# MAGIC # **_Part-1_**

# COMMAND ----------

# MAGIC %md
# MAGIC **_Spark Default Partitioning_**
# MAGIC - Spark uses the config spark.sql.files.maxPartitionBytes to decide the maximum partition size when reading files.
# MAGIC - Default value = 134217728 bytes = 128 MB.
# MAGIC - This means Spark tries to split large files into chunks of ~128 MB per partition. If files are smaller, multiple files can be combined into one partition.
# MAGIC
# MAGIC **_Automatic Partitioning_**
# MAGIC When you load a file (CSV, Parquet, ORC, etc.), Spark automatically determines how many partitions to create based on:
# MAGIC - File size
# MAGIC - spark.sql.files.maxPartitionBytes
# MAGIC - spark.sql.files.openCostInBytes (a small overhead cost added per file, default = 4 MB).
# MAGIC
# MAGIC **_Manual Control_**
# MAGIC
# MAGIC spark.conf.set("spark.sql.files.maxPartitionBytes", "256m")   # We can the change partition maunally.  
# MAGIC
# MAGIC spark.conf.get("spark.sql.files.maxPartitionBytes")   # For checking the patition maunally. 
# MAGIC
# MAGIC - Values can be in bytes (number) or with a suffix (b, k, m, g).
# MAGIC - After this, any new file you load will follow the updated partition size.
# MAGIC - (This setting does not retroactively change partitions already created.)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.get("spark.sql.adaptive.enabled")
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------

## Reading the source file from the DBFS

df_sales = (spark.read
           .format("csv")
          .option("inferschema", True)
          .option("header", True)
          .load("/FileStore/Optimization/Salesdata.csv"))

## --- Will Use the below datafames after words --- ##

## df_dim_product = (spark.read
##                  .format("csv")
##                  .option("inferschema", True)
##                  .option("header", True)
##                  .load("/FileStore/Optimization/dim_product.csv"))

##  df_dim_Customer = (spark.read
##                    .format("csv")
##                    .option("inferschema", True)
##                    .option("header", True)
##                   .load("/FileStore/Optimization/dim_customer.csv"))

# COMMAND ----------

display(df_sales)

# COMMAND ----------

## Checking the partition created by spark itslef. 
df_sales.rdd.getNumPartitions()

# COMMAND ----------

spark.conf.get("spark.sql.files.maxPartitionBytes")

# COMMAND ----------

# Changing the default partition size to 128kb each. 
spark.conf.set("spark.sql.files.maxPartitionBytes", 131072)

# COMMAND ----------

spark.conf.get("spark.sql.files.maxPartitionBytes")

# COMMAND ----------

## Again reading the sales datasame. 

df_sales = (spark.read
           .format("csv")
          .option("inferschema", True)
          .option("header", True)
          .load("/FileStore/Optimization/Salesdata.csv"))

# COMMAND ----------

## After maunally changing the partition to 128kb and we are Checking the partition created by spark. 
df_sales.rdd.getNumPartitions()

# COMMAND ----------

# Changing the default partition size to 128 MB
spark.conf.set("spark.sql.files.maxPartitionBytes", 134217728)

# Checking the partition
spark.conf.get("spark.sql.files.maxPartitionBytes")


# COMMAND ----------

# MAGIC %md
# MAGIC # **_Part-2_**

# COMMAND ----------

# MAGIC %md
# MAGIC **_Repartitioning in Spark_**
# MAGIC
# MAGIC - By default, Spark decides the number of partitions when you read a file or perform transformations.
# MAGIC - But sometimes you may want to change the number of partitions manually for performance tuning (e.g., balancing data across executors, avoiding small files, optimizing shuffle).
# MAGIC
# MAGIC **_repartition()_**
# MAGIC
# MAGIC - Creates exactly the number of partitions you specify.
# MAGIC - It always triggers a full shuffle of the data (expensive but ensures even distribution).
# MAGIC     > df = df.repartition(10)       # repartition into 10 partitions
# MAGIC     
# MAGIC     > df = df.repartition("col1")   # repartition based on values of a column
# MAGIC     
# MAGIC     > df = df.repartition(5, "col1", "col2")  # by multiple columns
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

df_sales_repart = (spark.read
           .format("csv")
          .option("inferschema", True)
          .option("header", True)
          .load("/FileStore/Optimization/Salesdata.csv"))

# COMMAND ----------

df_sales_repart.rdd.getNumPartitions()

# COMMAND ----------

## Examples based on df_sales. 

df = df_sales_repart.repartition(10)

# COMMAND ----------

## Checking the partition created
df.rdd.getNumPartitions()

# COMMAND ----------

## We can see now it has divided the datafame into 10 logical partition.

(df.withColumn("Partition_id", spark_partition_id())
   .groupBy("Partition_id")
   .count()
   .orderBy("Partition_id")
   .show())

# COMMAND ----------

## Writing these logical files in DBFS to check if its create the 10 partition or not.

(df.write
 .format("parquet")
 .option("path", "/FileStore/Optimization/parquetfiles/")
 .save())

# COMMAND ----------

## Reading the data now and if we go to spark UI it will show number of files read is 10
## This is because we are reading all the files. 

df_parquet = (spark.read.format("parquet")
              .load("/FileStore/Optimization/parquetfiles/"))

display(df_parquet)

# COMMAND ----------

## for the below exmaple also spark will check all the records since spark don't know in which partition we have product categort as food and it will read all the files which is 10 files. 

df_parquet = df_parquet.filter(col("ProductCategory") == 'Food')
display(df_parquet)

# COMMAND ----------

# MAGIC %md
# MAGIC # **_Scanning Optimization_**
# MAGIC
# MAGIC We explored the above examples to understand why optimization is required for our DataFrame. Depending on the use case, we need to choose the appropriate optimization technique.
# MAGIC
# MAGIC **Definition :**
# MAGIC Partitioning splits data into multiple directories (based on column values) when writing to disk, which helps in pruning unnecessary reads.
# MAGIC
# MAGIC **_Choosing Partition Column(s)_**
# MAGIC
# MAGIC - Pick columns that are frequently used in filters (WHERE clause).
# MAGIC - Avoid high-cardinality columns (like TransactionID) → creates too many small files.
# MAGIC - Best for categorical columns like Year, Month, Region, Department.
# MAGIC
# MAGIC
# MAGIC **_Best Practices_**
# MAGIC
# MAGIC - Small number of large files is better than too many small files.
# MAGIC - Combine partitionBy() with bucketBy() for better performance when partitioning alone is not enough.
# MAGIC - Good candidates:
# MAGIC - Date columns (Year/Month/Day)
# MAGIC - Region/Department/Category
# MAGIC - Avoid partitioning by continuous numeric values or unique IDs.

# COMMAND ----------

## This will create the parition based on the ProductCategort. And its is recommanded that we should create the partition based on columns which we are frequently using it. 

(df.write
 .format('parquet')
 .mode('append')
 .option('path' ,'/FileStore/Optimization/parquetfiles_opt')
 .partitionBy('ProductCategory')
 .save()) 
                                                  

# COMMAND ----------

# MAGIC %md
# MAGIC As we have create the partition based on the ProductCategory file will be generated as below.
# MAGIC - /parquetfiles_opt/ProductCategory=Electronics/
# MAGIC - /parquetfiles_opt/ProductCategory=Clothing/
# MAGIC - /parquetfiles_opt/ProductCategory=Furniture/
# MAGIC
# MAGIC And others based on the distinct ProductCategory

# COMMAND ----------

## Now making the calling the datafame and checking with the filters. 
df_parquet_ScanOptimized = (spark.read.format("parquet")
                            .load("/FileStore/Optimization/parquetfiles_opt"))

df = df_parquet_ScanOptimized.filter(col("ProductCategory") == 'Food')

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **When we compare the data before and after partitioning, we can see a significant difference in row read performance.**

# COMMAND ----------

# MAGIC %md
# MAGIC # **Conclusion**
# MAGIC
# MAGIC The comparison between pre- and post-partitioning clearly demonstrates that partitioning significantly enhances data read performance. By reducing the amount of data scanned and enabling targeted access, partitioning leads to faster query execution and better overall efficiency.
