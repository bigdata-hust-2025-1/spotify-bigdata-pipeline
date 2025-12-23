# spark_jobs/batch/export_tracks.py
import os
from pyspark.sql import SparkSession

# Lấy cấu hình từ biến môi trường (Docker truyền vào)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio.bigdata:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "miniopass123")

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

        # Đường dẫn ghi ra (Mount volume /data -> D:\Big_Data_For_School\data)
        output_dir = "/data/tracks_csv_output"

        print(f"--> Writing CSV to: {output_dir}")
        
        (
            df.coalesce(1) # Gom về 1 file duy nhất
              .write
              .option("header", "true")
              .mode("overwrite")
              .csv(output_dir)
        )

        print("✅ SUCCESS! Check your Windows folder: D:\\Big_Data_For_School\\data\\tracks_csv_output")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    main()