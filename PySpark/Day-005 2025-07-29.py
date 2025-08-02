# Databricks notebook source
# MAGIC %run ./MasterNoteBooks/INC_MasterNoteBook

# COMMAND ----------

inc = load_INC()

a = load_INC().limit(5)
display(a)

# COMMAND ----------


timestamp_expr = to_timestamp(lit("2024-09-29"), "yyyy-MM-dd")

# Format the timestamp to desired format
formatted_date_df = spark.range(1).select(date_format(timestamp_expr, "MM/dd/yyyy HH:mm:ss").alias("formatted_date"))

# Extract the formatted string into a Python variable
formatted_date_value = formatted_date_df.first()["formatted_date"]
print("Formatted date:", formatted_date_value)


# COMMAND ----------

# Identify customers who logged their first ticket in the last 30 days.

inc = load_INC()

timestamp_expr = to_timestamp(lit("2024-09-29"), "yyyy-MM-dd")

# Format the timestamp to desired format
formatted_date_df = spark.range(1).select(date_format(timestamp_expr, "MM/dd/yyyy HH:mm:ss").alias("formatted_date"))

# Extract the formatted string into a Python variable
formatted_date_value = formatted_date_df.first()["formatted_date"]


window_val = Window.partitionBy("Customer Name").orderBy(desc("Opened"))

CustomerLoggedTickets = (inc.withColumn("FirstTicket",row_number().over(window_val))
                         .withColumn("Opened",expr("try_to_timestamp(Opened,'MM/dd/yyyy HH:mm:ss')"))
                         .withColumn("Resolved",expr("try_to_timestamp(Resolved,'MM/dd/yyyy HH:mm:ss')"))
                         .select("Issue","Customer Name","Opened","Resolved")
                         .na.drop(subset=["Opened","Resolved"])
                         .where("FirstTicket == 1")
                         .filter(col("Opened") > to_timestamp(lit(formatted_date_value), "MM/dd/yyyy HH:mm:ss")))

display(CustomerLoggedTickets)

# COMMAND ----------

# Calculate a 7-day rolling average of ticket volume per Region.

inc = load_INC()

rolloing = (inc
            .groupBy("Region","Opened").count()
            )
display(rolloing)


# COMMAND ----------

## Pivot the data to show the count of tickets per Priority for each Region.

inc = load_INC()

pivotdata = inc.groupBy("Priority").pivot("Region").count()
display(pivotdata)

# COMMAND ----------

# Data Normalization
# Normalize the resolution time column using z-score normalization for modeling purposes.

inc = load_INC()

inc = inc.withColumn("Resolved_Seconds", unix_timestamp(col("Resolved"), "MM-dd-yyyy HH:mm"))

mean_data = (inc.groupBy("Region")
             .agg(mean(col("Resolved_Seconds")).alias("Mean_Resolved"),
                  stddev(col("Resolved_Seconds")).alias("Std_Deviation")))

display(mean_data)

# COMMAND ----------

df = (spark
      .read.format("csv").option("path","/Volumes/workspace/default/pyspark_source/Promo/")
      .option("inferSchema",True)
      .option("header",True).load())
display(df)

