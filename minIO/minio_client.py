# minio_client.py
from minio import Minio

MINIO_ENDPOINT = "localhost:9000"   # nếu sau này chạy trong Docker, đổi thành "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minio123"
MINIO_SECURE = False                # False nếu dùng http

def get_minio_client() -> Minio:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    return client
