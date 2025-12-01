from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("BronzeToSilverPlaylists") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio.bigdata:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "miniopass123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Đọc dữ liệu playlists từ lớp Bronze
    df_raw = spark.read.option("multiline", "true").json("s3a://spotify-bronze/playlists.json")

    # Transform: Lấy các trường cần thiết, bao gồm cả các khóa ngoại
    df_silver = df_raw.select(
        col("id").alias("playlist_id"),
        col("name").alias("playlist_name"),
        col("description"),
        col("followers.total").alias("followers"),
        col("owner.id").alias("owner_id") # Lấy khóa ngoại đến bảng owners
    ).dropna(subset=["playlist_id"])

    # Ghi dữ liệu vào lớp Silver
    df_silver.write \
        .mode("overwrite") \
        .parquet("s3a://spotify-silver/playlists/")

    print("Job BronzeToSilverPlaylists đã hoàn thành thành công!")
    spark.stop()

if __name__ == "__main__":
    main()