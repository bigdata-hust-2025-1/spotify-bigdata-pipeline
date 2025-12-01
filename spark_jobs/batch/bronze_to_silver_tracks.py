# spark_jobs/batch/bronze_to_silver_tracks.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

def main():
    spark = SparkSession.builder \
        .appName("BronzeToSilverTracks") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio.bigdata:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "miniopass123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Đọc dữ liệu tracks từ lớp Bronze
    df_raw = spark.read.option("multiline", "true").json("s3a://spotify-bronze/tracks.json")

    # Transform: Tương tự albums, cần explode mảng artists và lấy các khóa ngoại
    df_with_exploded_artists = df_raw.withColumn("artist_struct", explode(col("artists")))

    df_silver = df_with_exploded_artists.select(
        col("id").alias("track_id"),
        col("name").alias("track_name"),
        col("popularity"),
        col("duration_ms"),
        col("explicit"),
        col("track_number"),
        col("album.id").alias("album_id"), # Lấy khóa ngoại album
        col("artist_struct.id").alias("artist_id") # Lấy khóa ngoại artist
    ).dropna(subset=["track_id", "album_id", "artist_id"])

    # Ghi dữ liệu vào lớp Silver
    df_silver.write \
        .mode("overwrite") \
        .parquet("s3a://spotify-silver/tracks/")

    print("Job BronzeToSilverTracks đã hoàn thành thành công!")
    spark.stop()

if __name__ == "__main__":
    main()