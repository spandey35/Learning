# Databricks notebook source
# MAGIC %run /Workspace/Users/surajss.3110@gmail.com/Learning/PySpark/MasterNoteBooks/INC_MasterNoteBook

# COMMAND ----------

# Taking the water markvalues from etl_configuration table.
inc = load_INC()

etl_max_date = (spark
      .sql("Select Value from workspace.dbo.etl_configuration where Value_Class='Inc-Data' and IsActive=1 ")
      .collect()[0][0])       
## print(df)

data = (inc
        .withColumn("Opened", expr("try_to_timestamp(Opened, 'MM/dd/yyyy HH:mm:ss')"))
        .withColumn("Resolved", expr("try_to_timestamp(Resolved, 'MM/dd/yyyy HH:mm:ss')"))
        .na.drop(subset=["Opened","Resolved"])
        .select("Issue","Summary","Opened","Resolved","Status","Group Assignee")
        )
get_max_openeddate = data.select(max(data.Opened)).collect()[0][0]


if get_max_openeddate > etl_max_date:
      load = (data.filter(col("Opened") > get_max_openeddate))
      display(load)

      new_value_Etl_confi = (data.withColumn("Opened",col("Opened"))
                       .agg(max("Opened"))
                       .collect()[0][0])
      
      spark.sql(f"""UPDATE workspace.dbo.etl_configuration
        SET Value = '{new_value_Etl_confi}'
        WHERE Value_Class = 'Inc-Data' AND IsActive = 1""")
      
      dbutils.notebook.exit ("Watermark updated and new data processed.")
         
else:
     dbutils.notebook.exit("No data present for processing of incident files")



