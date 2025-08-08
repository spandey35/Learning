# Databricks notebook source
# MAGIC %md
# MAGIC ## 🟢 Intermediate Level – ETL & Basic Analytics
# MAGIC
# MAGIC ### 1. Join All Datasets
# MAGIC - Load the 3 CSV files in PySpark.
# MAGIC - Join on `customer_id` and `product_id` to make one combined dataset with:
# MAGIC   - **Customer full name**
# MAGIC   - **Product name**
# MAGIC   - **Transaction details**
# MAGIC
# MAGIC ### 2. Create a New Column
# MAGIC - Add a column:  
# MAGIC   `total_amount = price × quantity`
# MAGIC
# MAGIC ### 3. Top Customers by Spending
# MAGIC - Find the **top 5 customers** based on total spending.
# MAGIC
# MAGIC ### 4. Category Sales Summary
# MAGIC - For each category, calculate **total revenue**.
# MAGIC
# MAGIC ### 5. Filter by Date
# MAGIC - Show all transactions from the **year 2023 only**.
# MAGIC
# MAGIC ### 6. City-Based Sales Report
# MAGIC - For each city, calculate:
# MAGIC   - Number of transactions
# MAGIC   - Total revenue
# MAGIC   - Average order value
# MAGIC
# MAGIC ### 7. Category + Spending Filter
# MAGIC - Find customers who purchased **Electronics**  
# MAGIC   **AND** spent more than `$2000` in total.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🟠 Hard Level – Window Functions & Complex Aggregations
# MAGIC
# MAGIC ### 1. Monthly Sales Trend
# MAGIC - For each month, calculate:
# MAGIC   - Total revenue
# MAGIC   - Number of transactions
# MAGIC   - Average quantity sold
# MAGIC
# MAGIC ### 2. Top Product per Category
# MAGIC - Find the **product with the highest total sales** in each category.
# MAGIC
# MAGIC ### 3. Customer Ranking
# MAGIC - Rank customers by **total spending** using `dense_rank()`.
# MAGIC
# MAGIC ### 4. Repeat Customers
# MAGIC - Identify customers who made **more than 5 purchases** in the last 6 months.
# MAGIC
# MAGIC ### 5. Inactive Customers
# MAGIC - Find customers with **no purchases** in the last 6 months.
# MAGIC
# MAGIC ### 6. Top Customer in Each City
# MAGIC - Find the **highest spending customer** for every city.
# MAGIC
# MAGIC ### 7. Brand Revenue Share
# MAGIC - For each brand, calculate its **% share** of total revenue.
# MAGIC
# MAGIC ### 8. Low Stock & High Demand
# MAGIC - List products with:
# MAGIC   - Stock less than **50**
# MAGIC   - Quantity sold more than **100**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔴 Pro Level – Advanced Analytics & Data Engineering
# MAGIC
# MAGIC ### 1. Rolling Average Sales
# MAGIC - For each product, calculate a **7-day rolling average** of daily sales.
# MAGIC
# MAGIC ### 2. Customer Lifetime Value (CLV)
# MAGIC - For each customer:
# MAGIC   - Total spending
# MAGIC   - Average order value
# MAGIC   - Purchase frequency  
# MAGIC - Then calculate:  
# MAGIC   `CLV = Avg Order Value × Purchase Frequency × Customer Lifespan (years)`
# MAGIC
# MAGIC ### 3. RFM Analysis
# MAGIC - Compute **Recency**, **Frequency**, and **Monetary** scores.
# MAGIC - Rank customers based on these metrics.
# MAGIC
# MAGIC ### 4. Market Basket Analysis
# MAGIC - For each transaction, find **pairs of products** frequently bought together.
# MAGIC
# MAGIC ### 5. Category Seasonality
# MAGIC - For each category, identify **peak sales months** over the last 2 years.
# MAGIC
# MAGIC ### 6. Supplier Performance Report
# MAGIC - For each supplier:
# MAGIC   - Number of products supplied
# MAGIC   - Total revenue generated
# MAGIC   - Best-selling product
# MAGIC
# MAGIC ### 7. Customer Segmentation (Spark ML)
# MAGIC - Use:
# MAGIC   - Total spending
# MAGIC   - Purchase frequency
# MAGIC   - Loyalty points  
# MAGIC - Cluster customers into **3 groups**.
# MAGIC
# MAGIC ### 8. Daily Revenue Change %
# MAGIC - For each day, calculate the **% change in revenue** compared to the previous day.
# MAGIC
# MAGIC ### 9. Data Quality Check
# MAGIC - Identify:
# MAGIC   - Missing values
# MAGIC   - Invalid values (negative prices, null customer IDs, etc.)
# MAGIC
# MAGIC ### 10. Delta Lake Partitioned Storage
# MAGIC - Save processed transactions as a **Delta table**, partitioned by `year` and `month`.
# MAGIC
