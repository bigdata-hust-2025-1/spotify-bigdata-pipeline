from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("BronzeToSilverOwners") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio.bigdata:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "miniopass123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Đọc dữ liệu owners từ lớp Bronze
    df_raw = spark.read.option("multiline", "true").json("s3a://spotify-bronze/owners.json")

    # Transform: Chọn và đổi tên cột
    df_silver = df_raw.select(
        col("id").alias("owner_id"),
        col("display_name").alias("owner_name")
    ).dropna(subset=["owner_id"])

    # Ghi dữ liệu vào lớp Silver
    df_silver.write \
        .mode("overwrite") \
        .parquet("s3a://spotify-silver/owners/")

    print("Job BronzeToSilverOwners đã hoàn thành thành công!")
    spark.stop()

if __name__ == "__main__":
    main()