# spark_jobs/batch/bronze_to_silver_artists.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("BronzeToSilverArtists") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio.bigdata:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "miniopass123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Đọc dữ liệu artists từ lớp Bronze
    df_raw = spark.read.option("multiline", "true").json("s3a://spotify-bronze/artists.json")

    # Transform: Chọn cột, đổi tên, và truy cập trường lồng nhau
    df_silver = df_raw.select(
        col("id").alias("artist_id"),
        col("name").alias("artist_name"),
        col("genres"),
        col("popularity"),
        col("followers.total").alias("followers")
    ).dropna(subset=["artist_id"])

    # Ghi dữ liệu đã làm sạch vào lớp Silver dưới định dạng Parquet
    df_silver.write \
        .mode("overwrite") \
        .parquet("s3a://spotify-silver/artists/")
        
    print("Job BronzeToSilverArtists đã hoàn thành thành công!")
    spark.stop()

if __name__ == "__main__":
    main()