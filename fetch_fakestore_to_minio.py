# fetch_fakestore_to_minio.py
import requests, json, logging, os
from minio import Minio
from datetime import datetime
from io import BytesIO

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS", "admin")
MINIO_SECRET = os.getenv("MINIO_SECRET", "admin123")
RAW_BUCKET = "raw-bucket"

client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)

API_BASE = "https://fakestoreapi.com"

def upload_json_to_minio(obj_bytes, object_name):
    client.put_object(RAW_BUCKET, object_name, BytesIO(obj_bytes), length=len(obj_bytes), content_type="application/json")

def fetch_and_upload(endpoint_name):
    url = f"{API_BASE}/{endpoint_name}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{endpoint_name}/{endpoint_name}_{timestamp}.json"
    upload_json_to_minio(json.dumps(data).encode("utf-8"), filename)
    logging.info(f"Uploaded {filename} to {RAW_BUCKET}")
    return filename

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # endpoints: products, carts, users
    files = {}
    for ep in ["products", "carts", "users"]:
        files[ep] = fetch_and_upload(ep)
    print("Uploaded files:", files)
