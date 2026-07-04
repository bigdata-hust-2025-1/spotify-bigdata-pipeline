# minio_client.py
import os
import sys

from minio import Minio

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.config import require_env  # noqa: E402

MINIO_ENDPOINT = "localhost:9000"   # nếu sau này chạy trong Docker, đổi thành "minio:9000"
MINIO_ACCESS_KEY = require_env("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = require_env("MINIO_SECRET_KEY")
MINIO_SECURE = False                # False nếu dùng http

def get_minio_client() -> Minio:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    return client
