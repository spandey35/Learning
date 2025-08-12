# Databricks notebook source
# MAGIC %run /Workspace/Users/surajss.3110@gmail.com/Learning/PySpark/MasterNoteBooks/Sales_MasterNoteBook
# MAGIC

# COMMAND ----------

trans = load_trans()
prod = load_product()
cust = load_customer()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, round, avg

df = (trans
      .join(prod, "product_id")
      .withColumn("sales", round(col("price") * col("quantity"), 2))
      .orderBy(col("transaction_date").desc()))


window_spec = Window.partitionBy("product_id").orderBy("transaction_date").rowsBetween(-6, 0)

df_rolling = df.withColumn("rolling_mean", avg(col("sales")).over(window_spec)).select("product_id", "sales", "rolling_mean")

display(df_rolling)


# COMMAND ----------

from pyspark.sql.functions import col, round, sum, count

df = (trans
      .join(cust, "customer_id")
      .withColumn("sales", round(col("price") * col("quantity"), 2))
      .groupBy("customer_id")
      .agg(
          sum("sales").alias("Total_Spending"),
          (sum("sales") / count("*")).alias("Average_Order_Value"),
          count("*").alias("Purchase_Frequency")
      )
     )

display(df)


# COMMAND ----------

# Category Seasonality
# For each category, identify peak sales months over the last 2 years.

df = (trans
      .join(prod , "product_id")
      .withColumn("Sales", round(col("price") * col("quantity"),2))
      .withColumn("Year", year("transaction_date"))
      .withColumn("MonthName", monthname("transaction_date"))
      .filter(year("transaction_date") >= year(current_date()) - 2)
      .select("customer_id", trans.category.alias("category"), "Sales", "Year", "MonthName")
      )

monthly_sales = (df.groupBy("category", "Year", "MonthName")
                 .agg(sum("Sales").alias("TotalSales")))      

windowspec = Window.partitionBy("category").orderBy(col("Sales").desc())   

final = (df.withColumn("Rank", rank().over(windowspec))
         .filter("Rank =1 ")
         .select("customer_id", "category", "Sales", "Year", "MonthName"))

display(final)




# COMMAND ----------

# 8. Daily Revenue Change %
# For each day, calculate the % change in revenue compared to the previous day.

df = (trans
      .withColumn("Revenue", round(col("price") * col("quantity"),2))
      .na.drop(subset=["category", "product_id", "Revenue", "transaction_date"])
      .select("category", "product_id", "Revenue", "transaction_date")
      .orderBy("category","product_id", "transaction_date")
      )

window_spec = Window.partitionBy("product_id").orderBy("transaction_date")

df_with_lag = (df
               .withColumn("Lag_value", lag("Revenue", 1, 0).over(window_spec))
               .withColumn("%_Change", try_divide((col("Revenue") - col("Lag_value")), col("Lag_value")) * 100 )
               )

display(df_with_lag)

# COMMAND ----------


