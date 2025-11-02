from __future__ import annotations
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk import task
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sqlalchemy import create_engine

# Directories
RAW_DIR = "/opt/airflow/data/raw"
PROCESSED_DIR = "/opt/airflow/data/processed"
VIS_DIR = "/opt/airflow/data/visualizations"

os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(VIS_DIR, exist_ok=True)

# Spark session
spark = SparkSession.builder.appName("AirflowPySparkPipeline").getOrCreate()

# DAG default arguments
default_args = {
    "owner": "IDS706",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="customer_order_pipeline_taskflow",
    start_date=datetime(2025, 10, 1),
    schedule="@once",
    catchup=False,
    default_args=default_args,
) as dag:

    @task
    def extract_customers():
        df = pd.read_csv(os.path.join(RAW_DIR, "Customer.csv"))
        df.to_csv(os.path.join(PROCESSED_DIR, "customers_raw.csv"), index=False)

    @task
    def extract_orders():
        df = pd.read_csv(os.path.join(RAW_DIR, "Order.csv"))
        df.to_csv(os.path.join(PROCESSED_DIR, "orders_raw.csv"), index=False)

    @task
    def transform_customers():
        df = spark.read.csv(os.path.join(PROCESSED_DIR, "customers_raw.csv"), header=True, inferSchema=True)
        df = df.fillna({"age": 0})
        df = df.withColumn("Full_Name", col("Full Name"))
        df.write.csv(os.path.join(PROCESSED_DIR, "customers_clean.csv"), header=True, mode="overwrite")

    @task
    def transform_orders():
        df = spark.read.csv(os.path.join(PROCESSED_DIR, "orders_raw.csv"), header=True, inferSchema=True)
        df = df.filter(col("Order Status") != "CANCELLED")
        df = df.withColumn("Order_Total", col("Order Total").cast("double"))
        df.write.csv(os.path.join(PROCESSED_DIR, "orders_clean.csv"), header=True, mode="overwrite")

    @task
    def merge_data():
        customers_df = pd.read_csv(os.path.join(PROCESSED_DIR, "customers_clean.csv"))
        orders_df = pd.read_csv(os.path.join(PROCESSED_DIR, "orders_clean.csv"))
        merged = pd.merge(orders_df, customers_df, left_on="Id", right_on="Id", how="inner")
        merged.to_csv(os.path.join(PROCESSED_DIR, "merged_data.csv"), index=False)

    @task
    def load_to_postgres():
        engine = create_engine("postgresql+psycopg2://vscode:vscode@db:5432/airflow_db")
        merged_df = pd.read_csv(os.path.join(PROCESSED_DIR, "merged_data.csv"))
        merged_df.to_sql("merged_data", engine, if_exists="replace", index=False)

    @task
    def analyze_data():
        df = pd.read_csv(os.path.join(PROCESSED_DIR, "merged_data.csv"))
        plt.figure(figsize=(8, 5))
        sns.barplot(x="gender", y="Order Total", data=df, ci=None)
        plt.title("Average Order Total by Gender")
        plt.ylabel("Average Order Total")
        plt.savefig(os.path.join(VIS_DIR, "avg_order_total_by_gender.png"))
        plt.close()

    @task
    def cleanup():
        for f in ["customers_raw.csv", "orders_raw.csv", "customers_clean.csv", "orders_clean.csv"]:
            path = os.path.join(PROCESSED_DIR, f)
            if os.path.exists(path):
                os.remove(path)

    # Task dependencies
    customers = extract_customers()
    orders = extract_orders()
    t_cust = transform_customers()
    t_order = transform_orders()
    merged = merge_data()
    load = load_to_postgres()
    analyze = analyze_data()
    clean = cleanup()

    [customers, orders] >> [t_cust, t_order] >> merged >> load >> analyze >> clean
