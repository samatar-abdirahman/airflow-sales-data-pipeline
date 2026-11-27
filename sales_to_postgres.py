
import pandas as pd

from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="sales_to_postgres",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sales", "postgres"],
)
def sales_pipeline():

    @task
    def clean_and_load():

        # 1. Read CSV
        file_path = "/opt/airflow/dags/data/sales_data.csv"
        df = pd.read_csv(file_path)

        # 2. Clean data
        df = df.dropna(subset=["quantity"])
        df = df[df["quantity"] >= 0]
        df["order_date"] = pd.to_datetime(df["order_date"])

        # 3. Calculate total amount
        df["total_amount"] = df["quantity"] * df["price"]

        # 4. Prepare rows for PostgreSQL
        rows = [
            (
                row.order_id,
                row.order_date.date(),
                row.product,
                int(row.quantity),
                float(row.price),
                row.customer,
                float(row.total_amount),
            )
            for row in df.itertuples(index=False)
        ]

        # 5. Load data into PostgreSQL
        hook = PostgresHook(
            postgres_conn_id="sales_postgres"
        )

        hook.insert_rows(
            table="cleaned_sales",
            rows=rows,
            target_fields=[
                "order_id",
                "order_date",
                "product",
                "quantity",
                "price",
                "customer",
                "total_amount",
            ],
        )

        print(f"Successfully loaded {len(rows)} rows!")

    clean_and_load()


sales_pipeline()
