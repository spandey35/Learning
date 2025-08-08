# Databricks notebook source
# MAGIC %md
# MAGIC # 🐍 PySpark Practice Questions (Intermediate → Hard → Pro)
# MAGIC
# MAGIC ## **Intermediate Level**
# MAGIC 1. **Filter & Select**: Retrieve all transactions where the `total_amount` is greater than 500 and `payment_type` is "Credit Card".
# MAGIC 2. **Aggregation**: Find the total `total_amount` spent by each customer.
# MAGIC 3. **Sorting**: Get the top 10 customers who have spent the most, sorted in descending order.
# MAGIC 4. **Join**: Join `transactions` with `customers` to get the customer name along with transaction details.
# MAGIC 5. **Date Filtering**: Get all transactions from the last 3 months.
# MAGIC 6. **Group & Count**: Count the number of transactions for each `product_category`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## **Hard Level**
# MAGIC 7. **Multi-Join**: Join `transactions`, `customers`, and `products` to create a master dataset with customer, product, and transaction details.
# MAGIC 8. **Window Functions**: For each customer, rank their purchases by `total_amount` (highest first).
# MAGIC 9. **Aggregated Metrics**: For each product category, calculate the total sales amount, average price, and number of unique customers who purchased it.
# MAGIC 10. **Top-N by Group**: Find the top 3 most expensive products purchased by each customer.
# MAGIC 11. **Monthly Trends**: For each month, find the total sales and number of transactions.
# MAGIC 12. **Conditional Aggregation**: Calculate the total amount spent by each customer, separating totals for "Online" vs "Offline" orders.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## **Pro Level**
# MAGIC 13. **RFM Analysis**:
# MAGIC     - Recency: Days since last purchase
# MAGIC     - Frequency: Number of transactions
# MAGIC     - Monetary: Total spend
# MAGIC     - Generate RFM scores for each customer.
# MAGIC 14. **Basket Analysis**: Find products frequently bought together by the same customer in the same month.
# MAGIC 15. **Customer Lifetime Value (CLV)**: Calculate CLV for each customer assuming a fixed gross margin percentage.
# MAGIC 16. **Cohort Analysis**: Group customers by their first purchase month and analyze retention over the following months.
# MAGIC 17. **Sales Forecasting**: Using historical monthly sales data, create a simple PySpark-based moving average forecast.
# MAGIC 18. **Anomaly Detection**: Identify transactions where `total_amount` is more than 3 standard deviations above the mean for that customer.
# MAGIC 19. **Category Share Over Time**: For each month, calculate the percentage share of each product category in total sales.
# MAGIC 20. **Churn Prediction Feature Engineering**: Create a dataset of features (last purchase date, avg spend, visit frequency, etc.) for each customer to be used for churn prediction modeling.
# MAGIC
# MAGIC
