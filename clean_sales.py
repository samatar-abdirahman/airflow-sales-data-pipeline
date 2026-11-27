
import pandas as pd

# 1. Read CSV
file_path = "/opt/airflow/dags/data/sales_data.csv"
df = pd.read_csv(file_path)

print("Original rows:", len(df))

# 2. Remove missing quantity
df = df.dropna(subset=["quantity"])

# 3. Remove negative quantity
df = df[df["quantity"] >= 0]

# 4. Convert order_date to datetime
df["order_date"] = pd.to_datetime(df["order_date"])

# 5. Calculate total amount
df["total_amount"] = df["quantity"] * df["price"]

print("\nCleaned data:")
print(df)

print("\nCleaned rows:", len(df))
print("Missing values:")
print(df.isnull().sum())
