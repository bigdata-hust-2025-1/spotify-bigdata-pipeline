# spark_jobs/batch/bronze_to_silver_albums.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

def main():
    spark = SparkSession.builder \
        .appName("BronzeToSilverAlbums") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio.bigdata:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "miniopass123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Đọc dữ liệu albums từ lớp Bronze
    df_raw = spark.read.option("multiline", "true").json("s3a://spotify-bronze/albums.json")

    # Transform:
    # 1. Bóc tách (explode) mảng artists để mỗi dòng chỉ chứa một cặp album-artist
    # 2. Chọn và đổi tên các cột
    df_with_exploded_artists = df_raw.withColumn("artist_struct", explode(col("artists")))

    df_silver = df_with_exploded_artists.select(
        col("id").alias("album_id"),
        col("name").alias("album_name"),
        col("popularity"),
        col("release_date"),
        col("total_tracks"),
        col("artist_struct.id").alias("artist_id") # Lấy artist_id từ struct đã bóc tách
    ).withColumn("release_date", col("release_date").cast("date")) \
     .dropna(subset=["album_id", "artist_id"])
    
    # Ghi dữ liệu vào lớp Silver, phân vùng theo ngày phát hành để tối ưu truy vấn
    df_silver.write \
        .partitionBy("release_date") \
        .mode("overwrite") \
        .parquet("s3a://spotify-silver/albums/")

    print("Job BronzeToSilverAlbums đã hoàn thành thành công!")
    spark.stop()

if __name__ == "__main__":
    main()