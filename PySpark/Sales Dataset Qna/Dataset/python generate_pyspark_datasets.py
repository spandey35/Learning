import pandas as pd
import numpy as np
import random
import zipfile
import os

# Set save path
save_path = r"C:\Users\Suraj Pandey\OneDrive\Desktop"

# Create folder if it doesn't exist
os.makedirs(save_path, exist_ok=True)

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

# Dataset sizes
rows_transactions = 50000
rows_customers = 5000
rows_products = 500

# -------------------------------
# Generate Customers dataset
# -------------------------------
customers = [f"CUST{str(i).zfill(5)}" for i in range(1, rows_customers + 1)]
data_customers = {
    "customer_id": customers,
    "first_name": [f"FName{i}" for i in range(1, len(customers) + 1)],
    "last_name": [f"LName{i}" for i in range(1, len(customers) + 1)],
    "age": np.random.randint(18, 70, len(customers)),
    "gender": np.random.choice(["Male", "Female", "Other"], len(customers)),
    "signup_date": pd.to_datetime(np.random.randint(
        pd.Timestamp('2018-01-01').value // 10**9,
        pd.Timestamp('2022-12-31').value // 10**9,
        len(customers)
    ), unit='s'),
    "loyalty_points": np.random.randint(0, 5000, len(customers))
}
df_customers = pd.DataFrame(data_customers)

# -------------------------------
# Generate Products dataset
# -------------------------------
products = [f"PROD{str(i).zfill(4)}" for i in range(1, rows_products + 1)]
categories = ['Electronics', 'Clothing', 'Home', 'Books', 'Sports']
data_products = {
    "product_id": products,
    "product_name": [f"Product_{i}" for i in range(1, len(products) + 1)],
    "category": np.random.choice(categories, len(products)),
    "brand": [f"Brand_{i}" for i in range(1, len(products) + 1)],
    "stock": np.random.randint(0, 1000, len(products)),
    "supplier": [f"Supplier_{i}" for i in range(1, len(products) + 1)]
}
df_products = pd.DataFrame(data_products)

# -------------------------------
# Generate Transactions dataset
# -------------------------------
payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash on Delivery']
cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']

data_transactions = {
    "transaction_id": [f"TXN{str(i).zfill(7)}" for i in range(1, rows_transactions + 1)],
    "customer_id": np.random.choice(customers, rows_transactions),
    "product_id": np.random.choice(products, rows_transactions),
    "category": np.random.choice(categories, rows_transactions),
    "price": np.round(np.random.uniform(5, 500, rows_transactions), 2),
    "quantity": np.random.randint(1, 6, rows_transactions),
    "payment_method": np.random.choice(payment_methods, rows_transactions),
    "city": np.random.choice(cities, rows_transactions),
    "transaction_date": pd.to_datetime(np.random.randint(
        pd.Timestamp('2022-01-01').value // 10**9,
        pd.Timestamp('2023-12-31').value // 10**9,
        rows_transactions
    ), unit='s')
}
df_transactions = pd.DataFrame(data_transactions)

# -------------------------------
# Save datasets as CSV
# -------------------------------
transactions_file = os.path.join(save_path, "ecommerce_transactions_50k.csv")
customers_file = os.path.join(save_path, "customers.csv")
products_file = os.path.join(save_path, "products.csv")
zip_file = os.path.join(save_path, "pyspark_practice_datasets_50k.zip")

df_transactions.to_csv(transactions_file, index=False)
df_customers.to_csv(customers_file, index=False)
df_products.to_csv(products_file, index=False)

# -------------------------------
# Zip all CSVs together
# -------------------------------
with zipfile.ZipFile(zip_file, "w") as zipf:
    zipf.write(transactions_file, "ecommerce_transactions_50k.csv")
    zipf.write(customers_file, "customers.csv")
    zipf.write(products_file, "products.csv")

print(f"Datasets generated and saved to: {zip_file}")
