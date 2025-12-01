from minio import Minio
from minio_client import get_minio_client # CONFIG
import os

GOLD_BUCKET = "spotify-gold"

LOCAL_BASE_DIR = r"D:\PROJECT\Github\spotify-bigdata-pipeline\minIO\data\data_gold"  


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
