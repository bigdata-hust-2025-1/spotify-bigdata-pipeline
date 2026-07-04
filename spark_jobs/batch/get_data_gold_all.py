import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.config import DATA_DIR, require_env  # noqa: E402

GOLD_BUCKET = "spotify-gold"
# Thư mục local để export gold; mặc định repo-relative, override bằng GOLD_EXPORT_DIR.
LOCAL_BASE_DIR = os.getenv("GOLD_EXPORT_DIR", os.path.join(DATA_DIR, "data_gold"))
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio.bigdata:9000")
ACCESS_KEY = require_env("MINIO_ACCESS_KEY")
SECRET_KEY = require_env("MINIO_SECRET_KEY")


def main():

    # Khởi tạo SparkSession với cấu hình MinIO
    spark = SparkSession.builder \
        .appName("ExportMinIOToLocal") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

    os.makedirs(LOCAL_BASE_DIR, exist_ok=True)

    print(f"Đang export file từ bucket '{GOLD_BUCKET}' về '{LOCAL_BASE_DIR}' ...")

    base_path = f"s3a://{GOLD_BUCKET}"
    hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(base_path)

    # Lấy danh sách tất cả file trong bucket
    file_list = fs.listFiles(hdfs_path, True)

    count = 0
    while file_list.hasNext():
        status = file_list.next()
        file_path = status.getPath().toString()  # vd: s3a://spotify-gold/artists_stats/ingest_date=2025-11-29/data.parquet
        
        # Chuyển path s3a -> path local
        relative_path = file_path.replace(f"s3a://{GOLD_BUCKET}/", "")
        local_path = Path(LOCAL_BASE_DIR) / relative_path

        # Tạo folder local
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy file từ MinIO về local
        fs.copyToLocalFile(
            False,  # no overwrite flag
            spark._jvm.org.apache.hadoop.fs.Path(file_path),
            spark._jvm.org.apache.hadoop.fs.Path(str(local_path))
        )

        print(f"[DOWNLOAD] {file_path} -> {local_path}")
        count += 1

    print(f"\n✅ Đã export xong {count} file từ MinIO về thư mục: {LOCAL_BASE_DIR}")


if __name__ == "__main__":
    main()
