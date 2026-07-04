# spark_jobs/batch/export_tracks.py
import os
import sys

from pyspark.sql import SparkSession

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.config import require_env  # noqa: E402

# Lấy cấu hình từ biến môi trường (Docker truyền vào)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio.bigdata:9000")
ACCESS_KEY = require_env("MINIO_ACCESS_KEY")
SECRET_KEY = require_env("MINIO_SECRET_KEY")

# --- HÃY KIỂM TRA NGÀY TRÊN MINIO CỦA BẠN ---
INGEST_DATE = "2025-12-21" 

def main():
    spark = (
        SparkSession.builder
            .appName("ExportTracksCSV")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) 
            .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) 
            .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) 
            .config("spark.hadoop.fs.s3a.path.style.access", "true") 
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
    )

    # Đường dẫn đọc từ MinIO
    src_path = f"s3a://spotify-silver/tracks/ingest_date={INGEST_DATE}/"
    
    print(f"--> Reading from: {src_path}")

    try:
        df = spark.read.parquet(src_path)
        print(f"    Loaded {df.count()} rows, {len(df.columns)} columns")

        # Đường dẫn ghi ra (Mount volume /data từ thư mục dữ liệu local)
        output_dir = "/data/tracks_csv_output"

        print(f"--> Writing CSV to: {output_dir}")
        
        (
            df.coalesce(1) # Gom về 1 file duy nhất
              .write
              .option("header", "true")
              .mode("overwrite")
              .csv(output_dir)
        )

        print("✅ SUCCESS! Check your mounted output folder: /data/tracks_csv_output")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    main()