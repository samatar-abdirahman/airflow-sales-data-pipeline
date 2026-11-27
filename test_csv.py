
import pandas as pd

file_path = "/opt/airflow/dags/data/sales_data.csv"

df = pd.read_csv(file_path)

print("=== Missing Values ===")
print(df.isnull().sum())

print("\n=== Negative Quantity ===")
print(df[df["quantity"] < 0])

print("\n=== Dataset Information ===")
print(df.info())
