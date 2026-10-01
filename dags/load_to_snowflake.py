from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import boto3
import pandas as pd
from io import BytesIO
import logging


BUCKET = "nyc-taxi-data"
CLEANED_FILE = "cleaned/yellow_tripdata_2025-05-clean.parquet"


def load_to_snowflake():
    try:
        # Step 1: Read cleaned parquet from MinIO
        logging.info("📥 Reading data from MinIO...")
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="admin",
            aws_secret_access_key="admin123"
        )
        obj = s3.get_object(Bucket=BUCKET, Key=CLEANED_FILE)
        df = pd.read_parquet(BytesIO(obj["Body"].read()))
        logging.info(f"📊 DataFrame shape: {df.shape}")

        # Step 2: Connect to Snowflake via Airflow connection
        logging.info("🔌 Connecting to Snowflake via Airflow hook...")
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Step 3: Create table if not exists
        logging.info("🏗️ Creating table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS trips (
            vendor_id INT,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count INT,
            trip_distance FLOAT,
            fare_amount FLOAT,
            tip_amount FLOAT,
            total_amount FLOAT
        )
        """)

        # Step 4: Insert in batches
        batch_size = 10000
        total_rows = len(df)
        logging.info(f"⚡ Inserting {total_rows} rows in batches of {batch_size}...")

        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]

            batch_tuples = [
                (
                    int(row.get("VendorID")) if pd.notna(row.get("VendorID")) else None,
                    str(row.get("tpep_pickup_datetime")) if pd.notna(row.get("tpep_pickup_datetime")) else None,
                    str(row.get("tpep_dropoff_datetime")) if pd.notna(row.get("tpep_dropoff_datetime")) else None,
                    int(row.get("passenger_count")) if pd.notna(row.get("passenger_count")) else None,
                    float(row.get("trip_distance")) if pd.notna(row.get("trip_distance")) else None,
                    float(row.get("fare_amount")) if pd.notna(row.get("fare_amount")) else None,
                    float(row.get("tip_amount")) if pd.notna(row.get("tip_amount")) else None,
                    float(row.get("total_amount")) if pd.notna(row.get("total_amount")) else None
                )
                for _, row in batch_df.iterrows()
            ]

            cursor.executemany("""
                INSERT INTO trips (
                    vendor_id, pickup_datetime, dropoff_datetime,
                    passenger_count, trip_distance,
                    fare_amount, tip_amount, total_amount
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, batch_tuples)

            logging.info(f"📊 Batch {i//batch_size + 1}: {i+len(batch_df)}/{total_rows} rows inserted")

        conn.commit()
        cursor.close()
        conn.close()

        logging.info("🎉 Data loaded into Snowflake successfully!")

    except Exception as e:
        logging.error(f"❌ Error loading data: {str(e)}")
        raise


with DAG(
    dag_id="load_cleaned_data_to_snowflake",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["snowflake", "etl", "nyc-taxi"]
) as dag:

    load_task = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake
    )
