from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import logging, os, json, random
import pandas as pd
from minio import Minio
from io import BytesIO
import requests
from snowflake.connector.pandas_tools import write_pandas

# ================== CONFIG ==================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS", "admin")
MINIO_SECRET = os.getenv("MINIO_SECRET", "admin123")
RAW_BUCKET = "raw-bucket"
CLEAN_BUCKET = "cleaned-bucket"
ENDPOINTS = ["products", "carts", "users"]

default_args = {
    "owner": "vedant",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Helper functions
def random_order_date():
    # Random date from last 6 months
    return pd.Timestamp.utcnow() - pd.to_timedelta(random.randint(0, 180), unit='d')

def random_age():
    return random.randint(10, 60)

def get_season(date):
    m = date.month
    if m in [3, 4, 5]:
        return "Summer"
    elif m in [6, 7, 8]:
        return "Monsoon"
    elif m in [9, 10, 11]:
        return "Autumn"
    else:
        return "Winter"

# ================== DAG ==================
with DAG(
    dag_id="fake_store_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1
) as dag:

    # ================== FETCH TASK ==================
    def fetch_task(**kwargs):
        client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
        uploaded = {}
        for ep in ENDPOINTS:
            url = f"https://fakestoreapi.com/{ep}"
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            obj_name = f"{ep}/{ep}_{ts}.json"
            payload = json.dumps(data).encode('utf-8')
            client.put_object(RAW_BUCKET, obj_name, BytesIO(payload), len(payload), content_type="application/json")
            logging.info(f"Uploaded raw file: {obj_name}")
            uploaded[ep] = obj_name
        return uploaded

    # ================== TRANSFORM TASK ==================
    def transform_task(**kwargs):
        ti = kwargs['ti']
        uploaded = ti.xcom_pull(task_ids='fetch_data')
        client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
        cleaned_objects = {}

        for ep, obj_name in uploaded.items():
            resp = client.get_object(RAW_BUCKET, obj_name)
            data = json.loads(resp.read().decode('utf-8'))
            
            if ep == "carts":
                # CARTS SUMMARY
                carts_summary = []
                cart_items = []
                for cart in data:
                    for product in cart.get('products', []):
                        order_date = random_order_date()
                        quantity = product['quantity'] + random.randint(0, 5)
                        age = random_age()
                        season = get_season(order_date)

                        cart_items.append({
                            'cart_id': cart['id'],
                            'user_id': cart['userId'],
                            'cart_date': order_date,
                            'product_id': product['productId'],
                            'quantity': quantity,
                            'user_age': age,
                            'season': season
                        })

                    carts_summary.append({
                        'id': cart['id'],
                        'userId': cart['userId'],
                        'date': str(order_date)  # string for summary table
                    })

                # CART_ITEMS DF
                df_items = pd.DataFrame(cart_items)
                df_items.columns = [c.lower() for c in df_items.columns]
                df_items['ingested_at'] = pd.Timestamp.utcnow()

                buf_items = BytesIO()
                df_items.to_parquet(buf_items, index=False)
                buf_items.seek(0)
                items_object = obj_name.replace(f"{ep}/", f"cart_items/cleaned_").replace(".json", ".parquet")
                client.put_object(CLEAN_BUCKET, items_object, buf_items, length=len(buf_items.getvalue()), content_type="application/octet-stream")
                cleaned_objects['cart_items'] = items_object
                logging.info(f"Uploaded CART_ITEMS cleaned: {items_object}")

                # CARTS SUMMARY DF
                df = pd.DataFrame(carts_summary)

            elif ep == "products":
                df = pd.json_normalize(data)
                if 'rating.rate' in df.columns:
                    df = df.rename(columns={
                        "rating.rate": "rating_rate",
                        "rating.count": "rating_count"
                    })

            elif ep == "users":
                users_flat = []
                for user in data:
                    users_flat.append({
                        'id': user.get('id'),
                        'email': user.get('email'),
                        'username': user.get('username'),
                        'password': user.get('password'),
                        'name_first': user.get('name', {}).get('firstname'),
                        'name_last': user.get('name', {}).get('lastname'),
                        'address_city': user.get('address', {}).get('city'),
                        'address_street': user.get('address', {}).get('street'),
                        'address_number': user.get('address', {}).get('number'),
                        'address_zipcode': user.get('address', {}).get('zipcode'),
                        'geolocation_lat': user.get('address', {}).get('geolocation', {}).get('lat'),
                        'geolocation_long': user.get('address', {}).get('geolocation', {}).get('long'),
                        'phone': user.get('phone')
                    })
                df = pd.DataFrame(users_flat)

            # STANDARDIZE
            df.columns = [c.lower() for c in df.columns]
            df['ingested_at'] = pd.Timestamp.utcnow()

            buf = BytesIO()
            df.to_parquet(buf, index=False)
            buf.seek(0)
            cleaned_object = obj_name.replace(f"{ep}/", f"{ep}/cleaned_").replace(".json", ".parquet")
            client.put_object(CLEAN_BUCKET, cleaned_object, buf, length=len(buf.getvalue()), content_type="application/octet-stream")
            cleaned_objects[ep] = cleaned_object
            logging.info(f"Uploaded cleaned: {cleaned_object}")

        return cleaned_objects

    # ================== LOAD TASK ==================
    def load_task(**kwargs):
        ti = kwargs['ti']
        cleaned_objects = ti.xcom_pull(task_ids='transform_data')
        client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn_fakestore")
        conn = hook.get_conn()
        cursor = conn.cursor()

        def _sf_type_for_pd_series(s: pd.Series) -> str:
            if pd.api.types.is_integer_dtype(s) and not pd.api.types.is_bool_dtype(s):
                return "INTEGER"
            if pd.api.types.is_float_dtype(s):
                return "FLOAT"
            if pd.api.types.is_datetime64_any_dtype(s):
                return "TIMESTAMP_NTZ"
            return "STRING"

        def ensure_table_and_columns(table_name: str, df: pd.DataFrame):
            cursor.execute(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_NAME = '{table_name.upper()}' AND TABLE_SCHEMA = CURRENT_SCHEMA()"
            )
            rows = cursor.fetchall()
            existing = {r[0].upper() for r in rows} if rows else set()
            for col in df.columns:
                cname = col.upper()
                if cname not in existing:
                    ctype = _sf_type_for_pd_series(df[col])
                    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {cname} {ctype}")

        for ep, cleaned_obj in cleaned_objects.items():
            resp = client.get_object(CLEAN_BUCKET, cleaned_obj)
            df = pd.read_parquet(BytesIO(resp.read()))
            df.columns = [c.upper() for c in df.columns]

            if ep == "products":
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS PRODUCTS (
                    ID INTEGER,
                    TITLE STRING,
                    PRICE FLOAT,
                    CATEGORY STRING,
                    DESCRIPTION STRING,
                    IMAGE STRING,
                    RATING_RATE FLOAT,
                    RATING_COUNT INTEGER,
                    INGESTED_AT TIMESTAMP_NTZ
                )
                """)
                ensure_table_and_columns("PRODUCTS", df)
                cursor.execute("""CREATE OR REPLACE TEMP TABLE PRODUCTS_STAGING LIKE PRODUCTS""")
                write_pandas(conn, df, 'PRODUCTS_STAGING')

                cursor.execute("""
                MERGE INTO PRODUCTS T
                USING PRODUCTS_STAGING S
                ON T.ID = S.ID
                WHEN MATCHED THEN UPDATE SET
                    TITLE = S.TITLE,
                    PRICE = S.PRICE,
                    CATEGORY = S.CATEGORY,
                    DESCRIPTION = S.DESCRIPTION,
                    IMAGE = S.IMAGE,
                    RATING_RATE = S.RATING_RATE,
                    RATING_COUNT = S.RATING_COUNT,
                    INGESTED_AT = S.INGESTED_AT
                WHEN NOT MATCHED THEN
                INSERT (ID, TITLE, PRICE, CATEGORY, DESCRIPTION, IMAGE, RATING_RATE, RATING_COUNT, INGESTED_AT)
                VALUES (S.ID, S.TITLE, S.PRICE, S.CATEGORY, S.DESCRIPTION, S.IMAGE, S.RATING_RATE, S.RATING_COUNT, S.INGESTED_AT)
                """)

            elif ep == "carts":
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS CARTS (
                    ID INTEGER,
                    USERID INTEGER,
                    DATE STRING,
                    INGESTED_AT TIMESTAMP_NTZ
                )
                """)
                ensure_table_and_columns("CARTS", df)
                cursor.execute("CREATE OR REPLACE TEMP TABLE CARTS_STAGING LIKE CARTS")
                write_pandas(conn, df, 'CARTS_STAGING')
                cursor.execute("""
                MERGE INTO CARTS T
                USING CARTS_STAGING S
                ON T.ID = S.ID
                WHEN MATCHED THEN UPDATE SET
                    USERID = S.USERID,
                    DATE = S.DATE,
                    INGESTED_AT = S.INGESTED_AT
                WHEN NOT MATCHED THEN
                INSERT (ID, USERID, DATE, INGESTED_AT)
                VALUES (S.ID, S.USERID, S.DATE, S.INGESTED_AT)
                """)

            elif ep == "cart_items":
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS CART_ITEMS (
                    CART_ID INTEGER,
                    USER_ID INTEGER,
                    CART_DATE TIMESTAMP_NTZ,
                    PRODUCT_ID INTEGER,
                    QUANTITY INTEGER,
                    USER_AGE INTEGER,
                    SEASON STRING,
                    INGESTED_AT TIMESTAMP_NTZ
                )
                """)
                ensure_table_and_columns("CART_ITEMS", df)
                cursor.execute("CREATE OR REPLACE TEMP TABLE CART_ITEMS_STAGING LIKE CART_ITEMS")
                write_pandas(conn, df, 'CART_ITEMS_STAGING')

                # ⚠️ No delete this time — just keep inserting new rows
                cursor.execute("""
                INSERT INTO CART_ITEMS (CART_ID, USER_ID, CART_DATE, PRODUCT_ID, QUANTITY, USER_AGE, SEASON, INGESTED_AT)
                SELECT CART_ID, USER_ID, CART_DATE, PRODUCT_ID, QUANTITY, USER_AGE, SEASON, INGESTED_AT
                FROM CART_ITEMS_STAGING
                """)

            elif ep == "users":
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS USERS (
                    ID INTEGER,
                    EMAIL STRING,
                    USERNAME STRING,
                    PASSWORD STRING,
                    NAME_FIRST STRING,
                    NAME_LAST STRING,
                    ADDRESS_CITY STRING,
                    ADDRESS_STREET STRING,
                    ADDRESS_NUMBER STRING,
                    ADDRESS_ZIPCODE STRING,
                    GEOLOCATION_LAT STRING,
                    GEOLOCATION_LONG STRING,
                    PHONE STRING,
                    INGESTED_AT TIMESTAMP_NTZ
                )
                """)
                ensure_table_and_columns("USERS", df)
                cursor.execute("CREATE OR REPLACE TEMP TABLE USERS_STAGING LIKE USERS")
                write_pandas(conn, df, 'USERS_STAGING')
                cursor.execute("""
                MERGE INTO USERS T
                USING USERS_STAGING S
                ON T.ID = S.ID
                WHEN MATCHED THEN UPDATE SET
                    EMAIL = S.EMAIL,
                    USERNAME = S.USERNAME,
                    PASSWORD = S.PASSWORD,
                    NAME_FIRST = S.NAME_FIRST,
                    NAME_LAST = S.NAME_LAST,
                    ADDRESS_CITY = S.ADDRESS_CITY,
                    ADDRESS_STREET = S.ADDRESS_STREET,
                    ADDRESS_NUMBER = S.ADDRESS_NUMBER,
                    ADDRESS_ZIPCODE = S.ADDRESS_ZIPCODE,
                    GEOLOCATION_LAT = S.GEOLOCATION_LAT,
                    GEOLOCATION_LONG = S.GEOLOCATION_LONG,
                    PHONE = S.PHONE,
                    INGESTED_AT = S.INGESTED_AT
                WHEN NOT MATCHED THEN
                INSERT (ID, EMAIL, USERNAME, PASSWORD, NAME_FIRST, NAME_LAST, ADDRESS_CITY, ADDRESS_STREET, ADDRESS_NUMBER, ADDRESS_ZIPCODE, GEOLOCATION_LAT, GEOLOCATION_LONG, PHONE, INGESTED_AT)
                VALUES (S.ID, S.EMAIL, S.USERNAME, S.PASSWORD, S.NAME_FIRST, S.NAME_LAST, S.ADDRESS_CITY, S.ADDRESS_STREET, S.ADDRESS_NUMBER, S.ADDRESS_ZIPCODE, S.GEOLOCATION_LAT, S.GEOLOCATION_LONG, S.PHONE, S.INGESTED_AT)
                """)

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("✅ All tables loaded successfully!")

    fetch = PythonOperator(task_id="fetch_data", python_callable=fetch_task)
    transform = PythonOperator(task_id="transform_data", python_callable=transform_task)
    load = PythonOperator(task_id="load_to_snowflake", python_callable=load_task)

    fetch >> transform >> load
