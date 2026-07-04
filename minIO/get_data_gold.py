import os
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from minio_client import get_minio_client  # noqa: E402  (CONFIG)
from common.config import DATA_DIR  # noqa: E402

GOLD_BUCKET = "spotify-gold"

# Thư mục local để export; mặc định repo-relative, override bằng GOLD_EXPORT_DIR.
LOCAL_BASE_DIR = os.getenv("GOLD_EXPORT_DIR", os.path.join(DATA_DIR, "data_gold"))


def main():
    client = get_minio_client()

    # Tạo thư mục gốc nếu chưa có
    os.makedirs(LOCAL_BASE_DIR, exist_ok=True)

    print(f"Đang export toàn bộ file từ bucket '{GOLD_BUCKET}' về '{LOCAL_BASE_DIR}' ...")

    count = 0
    objects = client.list_objects(GOLD_BUCKET, recursive=True)

    for obj in objects:
        object_name = obj.object_name  # vd: artists_stats/ingest_date=2025-11-29/data.parquet

        # Bỏ qua "folder ảo" (nếu có)
        if object_name.endswith("/"):
            continue

        # Giữ nguyên cấu trúc folder khi export
        local_path = os.path.join(LOCAL_BASE_DIR, *object_name.split("/"))

        local_dir = os.path.dirname(local_path)
        os.makedirs(local_dir, exist_ok=True)

        print(f"[DOWNLOAD] s3://{GOLD_BUCKET}/{object_name} -> {local_path}")
        client.fget_object(GOLD_BUCKET, object_name, local_path)
        count += 1

    print(f"\n✅ Đã export xong {count} file từ MinIO về thư mục: {LOCAL_BASE_DIR}")


if __name__ == "__main__":
    main()
