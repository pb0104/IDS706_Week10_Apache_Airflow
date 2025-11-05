from __future__ import annotations
import csv
import os
import shutil
from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import Error as DatabaseError

# NOTE: This DAG assumes Orders.csv 'Id' refers to the customer Id (to join with Customers.csv Id).
OUTPUT_DIR = "/opt/airflow/data"
VIS_DIR = os.path.join(OUTPUT_DIR, "visualizations")
SCHEMA = "week8_demo"

default_args = {"owner": "IDS706", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="pipeline",
    start_date=datetime(2025, 10, 1),
    schedule="@once",
    catchup=False,
    default_args=default_args,
) as dag:

    @task()
    def read_customers_file(output_dir: str = OUTPUT_DIR) -> str:
        """Return path to Customers.csv (case-sensitive)."""
        filepath = os.path.join(output_dir, "Customers.csv")
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Customers file not found at {filepath}")
        print(f"Found customers file: {filepath}")
        return filepath

    @task()
    def read_orders_file(output_dir: str = OUTPUT_DIR) -> str:
        """Return path to Orders.csv (case-sensitive)."""
        filepath = os.path.join(output_dir, "Orders.csv")
        if not os.path.exists(filepath):
            # also try the singular/other common names for safety
            alt = os.path.join(output_dir, "Order.csv")
            if os.path.exists(alt):
                filepath = alt
            else:
                raise FileNotFoundError(f"Orders file not found at {filepath} or {alt}")
        print(f"Found orders file: {filepath}")
        return filepath

    @task()
    def load_csv_to_pg(conn_id: str, csv_path: str, table: str, schema: str = SCHEMA, append: bool = True) -> int:
        """
        Load CSV into Postgres under schema.table. Columns are created as TEXT to avoid strict type issues.
        Returns number of inserted rows.
        """
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            rows = [tuple((r.get(col, "") or None) for col in fieldnames) for r in reader]

        if not fieldnames:
            print(f"No header found in {csv_path}; nothing to load.")
            return 0

        create_schema = f"CREATE SCHEMA IF NOT EXISTS {schema};"
        create_table_cols = ", ".join([f'"{col}" TEXT' for col in fieldnames])
        create_table = f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({create_table_cols});'
        delete_rows = f'DELETE FROM "{schema}"."{table}";' if not append else None

        placeholders = ", ".join(["%s"] * len(fieldnames))
        columns = ", ".join([f'"{c}"' for c in fieldnames])
        insert_sql = f'INSERT INTO "{schema}"."{table}" ({columns}) VALUES ({placeholders});'


        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(create_schema)
                cur.execute(create_table)
                if delete_rows:
                    cur.execute(delete_rows)
                    print(f"Cleared existing rows in {schema}.{table}")
                if rows:
                    cur.executemany(insert_sql, rows)
                    conn.commit()
                    print(f"Inserted {len(rows)} rows into {schema}.{table}")
                    return len(rows)
                else:
                    print("No rows to insert.")
                    return 0
        except DatabaseError as e:
            print(f"Database error while loading {csv_path} to {schema}.{table}: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    @task()
    def create_visualization_from_db(conn_id: str, customers_table: str = "customers", orders_table: str = "orders", schema: str = SCHEMA, vis_dir: str = VIS_DIR) -> str:
        """
        Join customers and orders using Id, aggregate Order_Total by gender, and save bar chart PNG.
        Uses pandas + matplotlib inside the task (imported here to avoid import-time delays).
        """
        import pandas as pd
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        os.makedirs(vis_dir, exist_ok=True)
        out_path = os.path.join(vis_dir, "order_total_by_gender.png")

        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            # Detect actual 'Id' column in customers table
            customers_cols = pd.read_sql_query(
                f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = '{customers_table}';
                """, conn
            )["column_name"].tolist()
            orders_cols = pd.read_sql_query(
                f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = '{orders_table}';
                """, conn
            )["column_name"].tolist()

            # Pick the column that contains 'Id' (ignores hidden chars)
            customer_id_col = next((c for c in customers_cols if "Id" in c), None)
            order_id_col = next((c for c in orders_cols if "Id" in c), None)
            if not customer_id_col or not order_id_col:
                raise RuntimeError("Could not detect 'Id' columns in customers or orders table.")


            # Query that joins Customers.Id with Orders.Id (assumption: Orders.Id is customer id)
            sql = f'''
            SELECT c."{customer_id_col}" AS customer_id,
                   c."Full_Name",
                   c."age",
                   c."gender",
                   o."Order_Total"
            FROM "{schema}"."{customers_table}" c
            JOIN "{schema}"."{orders_table}" o
              ON c."{customer_id_col}" = o."{order_id_col}"
            WHERE o."Order_Status" IS NULL OR o."Order_Status" <> 'CANCELLED';
            '''
            df = pd.read_sql_query(sql, conn)
            if df.empty:
                raise RuntimeError("Joined query returned no rows — check that Orders.Id matches Customers.Id and files have data.")

            # Coerce Order_Total to numeric
            df["Order_Total"] = pd.to_numeric(df["Order_Total"], errors="coerce").fillna(0)

            # Group by gender and sum
            if "gender" in df.columns and df["gender"].notna().any():
                agg = df.groupby("gender", dropna=False)["Order_Total"].sum().reset_index()
                x = agg["gender"].astype(str)
                y = agg["Order_Total"]
                xlabel = "gender"
            else:
                # fallback: aggregate by age buckets if gender not available
                df["age"] = pd.to_numeric(df["age"], errors="coerce").fillna(-1).astype(int)
                bins = [0, 18, 30, 45, 60, 200]
                labels = ["<18", "18-29", "30-44", "45-59", "60+"]
                df["age_bucket"] = pd.cut(df["age"].replace(-1, pd.NA), bins=bins, labels=labels, right=False)
                agg = df.groupby("age_bucket")["Order_Total"].sum().reset_index().dropna()
                x = agg["age_bucket"].astype(str)
                y = agg["Order_Total"]
                xlabel = "age_bucket"

            plt.figure(figsize=(8, 5))
            plt.bar(x, y)
            plt.ylabel("Total Order Value")
            plt.xlabel(xlabel)
            plt.title("Total Order Value by " + xlabel)
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()
            plt.savefig(out_path)
            print(f"Saved visualization to {out_path}")
            return out_path
        finally:
            conn.close()

    @task()
    def clear_folder(folder_path: str = OUTPUT_DIR) -> None:
        """
        Delete files and directories inside data folder but preserve the visualizations folder.
        """
        if not os.path.exists(folder_path):
            print(f"Folder {folder_path} does not exist.")
            return

        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            # preserve the visualization directory
            if os.path.abspath(file_path) == os.path.abspath(VIS_DIR):
                print(f"Skipping visualization folder: {file_path}")
                continue
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)
                    print(f"Removed file: {file_path}")
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    print(f"Removed directory: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")

        print("Cleanup completed.")

    # --- DAG wiring ---
    customers_csv = read_customers_file()
    orders_csv = read_orders_file()

    load_customers = load_csv_to_pg(conn_id="Postgres", csv_path=customers_csv, table="customers", append=False)
    load_orders = load_csv_to_pg(conn_id="Postgres", csv_path=orders_csv, table="orders", append=False)

    viz = create_visualization_from_db(conn_id="Postgres", customers_table="customers", orders_table="orders")

    cleanup = clear_folder()

    # dependencies: load both tables -> viz -> cleanup
    [load_customers, load_orders] >> viz
    # >> cleanup
