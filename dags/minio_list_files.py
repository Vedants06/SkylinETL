from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
import boto3 # type: ignore

def list_minio_files():
    # Connect to MinIO (S3 API)
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",   # MinIO API endpoint
        aws_access_key_id="admin",
        aws_secret_access_key="admin123"
    )

    bucket_name = "nyc-taxi-data"
    objects = s3.list_objects_v2(Bucket=bucket_name)

    if "Contents" in objects:
        for obj in objects["Contents"]:
            print("✅ Found file:", obj["Key"])
    else:
        print("⚠️ No files found in bucket:", bucket_name)

# Airflow DAG definition
with DAG(
    dag_id="minio_list_files_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["minio", "s3", "nyc-taxi"]
) as dag:

    list_files_task = PythonOperator(
        task_id="list_files",
        python_callable=list_minio_files
    )
