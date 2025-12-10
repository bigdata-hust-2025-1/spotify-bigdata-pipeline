# spark_jobs/batch/ingest_bronze_spark.py
import os
from datetime import date
from pyspark.sql import SparkSession

def main():
    # 1. Khởi tạo Spark với cấu hình S3
    # Lưu ý: Các biến môi trường sẽ được truyền vào từ lệnh Docker
    spark = SparkSession.builder \
        .appName("IngestToBronze") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://host.docker.internal:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "miniopass123")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    print("Spark Session created for Ingestion!")

    # Cấu hình đường dẫn (Bên trong Docker)
    # Chúng ta sẽ mount thư mục D:\Big_Data_For_School\data vào /data trong container
    LOCAL_DATA_DIR_IN_DOCKER = "/data"
    BRONZE_BUCKET = "s3a://spotify-bronze"
    ingest_date = date.today().isoformat()

    # Danh sách file cần upload
    # Mapping: filename -> domain folder
    files_to_ingest = {
        "albums.json": "albums",
        "artists.json": "artists",
        "owners.json": "owners",
        "playlists.json": "playlists",
        "tracks.json": "tracks"
    }

    for filename, domain in files_to_ingest.items():
        local_path = os.path.join(LOCAL_DATA_DIR_IN_DOCKER, filename)
        
        # Kiểm tra xem file có tồn tại không (dùng Python os check cho nhanh)
        if not os.path.exists(local_path):
            print(f"[SKIP] File not found inside container: {local_path}")
            continue

        print(f"--- Processing: {filename} ---")
        
        # 2. Đọc file JSON Local bằng Spark
        # Option multiline=true quan trọng để đọc file JSON nhiều dòng
        df = spark.read.option("multiline", "true").json(local_path)
        
        # 3. Ghi vào MinIO (Bronze Layer)
        # Cấu trúc: domain/ingest_date=YYYY-MM-DD/
        output_path = f"{BRONZE_BUCKET}/{domain}/ingest_date={ingest_date}"
        
        print(f"[WRITING] {local_path} -> {output_path}")
        
        df.write \
            .mode("overwrite") \
            .json(output_path)  # Ghi lại dưới dạng JSON (chuẩn Bronze)
            
    print("=== INGESTION COMPLETED ===")
    spark.stop()

if __name__ == "__main__":
    main()