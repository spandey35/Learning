# Databricks notebook source
# MAGIC %run /Workspace/Users/surajss.3110@gmail.com/Learning/PySpark/MasterNoteBooks/Sales_MasterNoteBook

# COMMAND ----------

prod = load_product()
cust = load_customer()
trans = load_trans()


# COMMAND ----------

# Monthly Sales Trend

df = (trans
      .withColumn("TransactionYear", year(col("transaction_date")))
      .withColumn("TransactionMonth",  monthname(col("transaction_date")))
      .groupBy("TransactionYear", "TransactionMonth" )
      .agg(
          count("*").alias("Number of transactions"),
          sum(col("price") * col("quantity")).alias("Total revenue"),
          avg(col("price") * col("quantity")).alias("Average order value")
      )
      .orderBy ("TransactionYear"))

display(df)



# COMMAND ----------

# Top Product per Category
# Find the product with the highest total sales in each category.

raw = (trans
      .join (prod, prod.product_id == trans.product_id)
      .withColumn("Highest Total Sales", (col("price") * col("quantity")))
      .select(trans.category.alias("category"),trans.product_id.alias("product_id"),"Highest Total Sales")
      )

windowSpec  = Window.partitionBy("Category").orderBy(desc(raw["Highest Total Sales"]))

df = (raw.withColumn("Top_Product", rank().over(windowSpec))
      .filter("Top_Product ==1")
      .select("category","product_id","Highest Total Sales"))

display(df)

# COMMAND ----------

#  Customer Ranking
# Rank customers by total spending using dense_rank().

raw = (trans
       .join(cust , cust.customer_id  == trans.customer_id)
       .withColumn("Highest Total Sales", round((col("price") * col("quantity"))))
       .select(cust.customer_id.alias("customer_id"),"price" , "quantity", "Highest Total Sales")
       )

windowSpec = Window.partitionBy("customer_id").orderBy(desc(raw["Highest Total Sales"]))

df = (raw.withColumn("Rank", dense_rank().over(windowSpec))
      .select("customer_id", "price","quantity", "Highest Total Sales","Rank")
      )
display(df)      

# COMMAND ----------

# Repeat Customers
# Identify customers who made more than 5 purchases in the last 6 months.

six_months_ago = date_sub(current_date(), 180)

df = (trans.
      join(cust, "customer_id")
      .select(trans.customer_id.alias("customer_id"),"transaction_id","payment_method","transaction_date")
      .filter(col("transaction_date") >= six_months_ago)  
)

windowSpec = Window.partitionBy("customer_id").orderBy(desc("transaction_date"))

ranked_df = (df
             .withColumn("Ranked_Customer", rank().over(windowSpec))
             .groupBy("customer_id")
             .count()
             .withColumnRenamed("count", "purchase_count")
             .filter("purchase_count > 5")
             .orderBy(col("purchase_count").desc())
             )

display(ranked_df)


# COMMAND ----------

# Top Customer in Each City
# Find the highest spending customer for every city.

df = (trans
      .join(cust, "customer_id")
      .withColumn("Spending", round(col("price") *  col("quantity"),2))
      .select("customer_id", "city", "Spending")
      )

windowSpec = Window.partitionBy("city").orderBy(desc(df["Spending"]))

final = (df.withColumn("Highest_Spending", rank().over(windowSpec))
         .filter("Highest_Spending = 1")
         .orderBy(col("Highest_Spending").desc())
         .select("city", "customer_id", "Spending")
         )


display(final)          

# COMMAND ----------

# 7. Brand Revenue Share
# For each brand, calculate its % share of total revenue.



df = (trans.
      join(prod, "product_id")
      .withColumn("Revenue", round((col("price") *  col("quantity")),2))
      .groupBy(prod["category"]).agg(
          round(sum("Revenue"),2).alias("Revenue")
          )
      )

total_Revenue = df.agg(sum("Revenue").alias("Total_Revenue_Sum")).collect()[0][0]


final = (df
         .withColumn("Revenue_Share", (col("Revenue") / lit(total_Revenue)) * 100)
         .select("category", "Revenue_Share")
         .orderBy(col("Revenue_Share").desc())
        )     
display(final)



# COMMAND ----------

# 8. Low Stock & High Demand
# List products with:
# Stock less than 50
# Quantity sold more than 100

df = (trans
      .join(prod, "product_id")
      .filter("stock < 50")
      )
 

final = (df
         .groupBy("product_id", "product_name", "stock")
         .agg(sum("quantity").alias("total_quantity_sold"))
         .filter(col("total_quantity_sold") > 100)
)



display(df)      
