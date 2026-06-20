from pyspark.sql import SparkSession

# Cấu hình Spark session tích hợp Iceberg và MinIO
spark = SparkSession.builder \
    .appName("Bronze_to_Silver_Iceberg") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://datalake/warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "MINIO_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "MINIO_SECRET_KEY") \
    .getOrCreate()

def process_data():
    # Đọc dữ liệu Bronze từ MinIO (ví dụ: raw JSON files)
    print("Reading bronze data from MinIO...")
    df = spark.read.json("s3a://datalake/bronze/spotify_events/")
    
    # Transformation: Clean data, filter nulls
    df_cleaned = df.dropna()
    
    # Save dưới định dạng Iceberg vào lớp Silver
    print("Writing data to Silver layer in Iceberg format...")
    df_cleaned.write.format("iceberg") \
        .mode("append") \
        .save("spark_catalog.silver.spotify_events")
        
    print("Successfully written to Iceberg Lakehouse on MinIO")

if __name__ == "__main__":
    process_data()
