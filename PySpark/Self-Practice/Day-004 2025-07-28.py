# Databricks notebook source
# MAGIC %run ./MasterNoteBooks/INC_MasterNoteBook

# COMMAND ----------

help(timestamp_diff)

# COMMAND ----------

# 1. Multi-Level Window Functions
# Within each Country, rank tickets by resolution time, and then find the average rank per Cluster.

inc = load_INC()


expr_time =( inc
            .withColumn("Resolved", expr("try_to_timestamp(Resolved, 'MM/dd/yyyy HH:mm:ss')")) 
            .withColumn("Opened",expr("try_to_timestamp(Opened,'MM/dd/yyyy HH:mm:ss')"))
            )
calc_resolution_time = (expr_time
                        .withColumn("Resolution_time", timestamp_diff("MINUTE",col("Opened"),col("Resolved")))
                        .na.drop(subset=["Opened", "Resolved", "Resolution_time"])
                        .select("Issue","Summary","Priority","Country","Cluster","Opened","Resolved","Resolution_time")
                        )

window = Window.partitionBy("Country").orderBy(calc_resolution_time["Resolution_time"].desc())   

ranked = (calc_resolution_time.withColumn("Ranked_Resolution_time",rank().over(window))
          .groupBy("Cluster").agg(round(avg("Ranked_Resolution_time"),2).alias("Cluster_Avg_Rank"))
          )

display(ranked)


# COMMAND ----------

# 2. Data Quality Checks
# Detect and report anomalies such as missing Resolved dates for tickets marked as Closed.

inc = load_INC()


filtered = inc.filter((inc["Status"] == 'Closed') & (inc["Resolved"].isNull()))

display(filtered)

# COMMAND ----------

## 3. UDF Usage
## Create a UDF to classify tickets as “Quick Fix”, “Moderate”, or “Complex” based on resolution time and priority.

inc = load_INC()


rounded_times = (inc.withColumn("Resolved",expr("try_to_timestamp(Resolved,'MM/dd/yyyy HH:mm:ss')"))
                 .withColumn("Opened",expr("try_to_timestamp(Opened,'MM/dd/yyyy HH:mm:ss')"))
                 .withColumn("Diff_Resoltion_in_mins",timestamp_diff("Minute","Opened","Resolved"))
                 .na.drop(subset=["Opened","Resolved","Diff_Resoltion_in_mins"])
)

window = Window.partitionBy("Country").orderBy(rounded_times["Diff_Resoltion_in_mins"].desc())
           

a = (rounded_times.withColumn("Rank",rank().over(window))
     .select("Issue","Summary","Priority","Country","Cluster","Opened","Resolved","Diff_Resoltion_in_mins","Rank"))

def classifytickets(Minutes):
    if Minutes <= 1000:
        return "Quick fix"
    
    elif Minutes >=1001 and Minutes <=2000 :
        return "Moderate fix"
    
    else:
        return "Complex fix"
    
udf_val = udf(classifytickets,StringType())

final = (a.withColumn("Ticket Classification",udf_val("Diff_Resoltion_in_mins")))



display(final)     



# COMMAND ----------

## 4. Compute the average resolution time at three levels: Country, Cluster, and Region, and compare them.

from pyspark.sql.functions import expr, col, avg

# Step 1: Load and preprocess data once
inc = (load_INC()
       .withColumn("Resolved", expr("try_to_timestamp(Resolved,'MM/dd/yyyy HH:mm:ss')"))
       .withColumn("Opened", expr("try_to_timestamp(Opened,'MM/dd/yyyy HH:mm:ss')"))
       .withColumn("Resolution_time_mins", timestamp_diff("Minute", col("Opened"), col("Resolved")))
       .na.drop(subset=["Opened", "Resolved", "Resolution_time_mins"])
)


country_level = inc.groupBy("Country").agg(round(avg("Resolution_time_mins")).alias("Avg_Resolution_Country"))


cluster_level = inc.groupBy("Cluster").agg(round(avg("Resolution_time_mins")).alias("Avg_Resolution_Cluster"))


region_level = inc.groupBy("Region").agg(round(avg("Resolution_time_mins")).alias("Avg_Resolution_Region"))


display(country_level)
display(cluster_level)
display(region_level)
       
                                                 

# COMMAND ----------


