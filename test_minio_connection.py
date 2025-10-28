# file: test_minio_connection.py

from pyspark.sql import SparkSession

def main():
    """
    Script để kiểm tra kết nối giữa PySpark và MinIO.
    """
    print("Bắt đầu kiểm tra kết nối Spark-MinIO...")

    # 1. Khởi tạo SparkSession với cấu hình để kết nối S3 (API của MinIO)
    # Các cấu hình này rất quan trọng, bạn sẽ dùng lại trong tất cả các job sau này
    spark = SparkSession.builder \
        .appName("MinIO Connection Test") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    print("Spark Session đã được tạo thành công.")

    # 2. Tạo một DataFrame mẫu
    data = [("1", "Alice"), ("2", "Bob")]
    columns = ["id", "name"]
    df = spark.createDataFrame(data, columns)
    print("DataFrame mẫu đã được tạo.")
    df.show()

    # 3. Ghi DataFrame lên bucket 'spotify-bronze' trên MinIO
    bucket_name = "spotify-bronze"
    output_path = f"s3a://{bucket_name}/connection-test/"
    
    print(f"Bắt đầu ghi dữ liệu vào: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    print("Ghi dữ liệu lên MinIO thành công!")

    # 4. Đọc lại dữ liệu từ MinIO để xác nhận
    print(f"Bắt đầu đọc lại dữ liệu từ: {output_path}")
    df_read = spark.read.parquet(output_path)
    print("Đọc dữ liệu từ MinIO thành công. Dữ liệu đọc được:")
    df_read.show()

    spark.stop()
    print("Kiểm tra kết nối hoàn tất!")

if __name__ == '__main__':
    main()