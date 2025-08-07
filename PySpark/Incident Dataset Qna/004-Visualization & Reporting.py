# Databricks notebook source
# MAGIC %run /Workspace/Users/surajss.3110@gmail.com/Learning/PySpark/MasterNoteBooks/INC_MasterNoteBook

# COMMAND ----------

# Weekly Heatmap of Ticket Volume Create a matrix showing ticket volume by day of week and hour of day.
 
inc = load_INC()

brozne = (inc
          .withColumn("Opened", expr("try_to_timestamp(Opened, 'MM/dd/yyyy HH:mm:ss')"))
          .withColumn("Resolved", expr("try_to_timestamp(Resolved, 'MM/dd/yyyy HH:mm:ss')"))
          .na.drop(subset= ["Opened", "Resolved"])
          .withColumn("Res_Timings", round((unix_timestamp("Resolved") -unix_timestamp("Opened"))/3600))
          .withColumn("Day_of_week", date_format("Opened", "E"))
          .withColumn("Hour_of_day", hour("Opened"))
          .select("Issue","Priority","Opened","Resolved","Day_of_week","Hour_of_day","Res_Timings")
          )

silver = (brozne.groupBy("Day_of_week","Hour_of_day") .count()
          
          .orderBy(col("Day_of_week")))

Gold = silver.groupBy("Day_of_week").pivot("Hour_of_day").sum("count").fillna(0)
display(Gold)

       
  


# COMMAND ----------

# Assignment Group Performance Dashboard Generate summary metrics (avg resolution time, SLA breach rate, ticket count) per group for dashboarding.

inc = load_INC()

brozne = (inc
          .withColumn("Opened", expr("try_to_timestamp(Opened, 'MM/dd/yyyy HH:mm:ss')"))
          .withColumn("Resolved", expr("try_to_timestamp(Resolved, 'MM/dd/yyyy HH:mm:ss')"))
          .withColumn("Resolution Time (In Hours)", round((unix_timestamp("Resolved") - unix_timestamp("Opened"))/3600))
          .na.drop(subset=["Opened", "Resolved"])
          .select("Issue", "Opened", "Resolved", "Resolution Time (In Hours)","Country Group")
          )




silver = (brozne.
          groupBy("Country Group")
          .agg(count("Issue").alias("Ticket Count"),
               round(avg("Resolution Time (In Hours)"), 2).alias("Avg_Resolution Time (In Hours)"))
          .orderBy(col("Avg_Resolution Time (In Hours)"))
          )

display(silver)          
