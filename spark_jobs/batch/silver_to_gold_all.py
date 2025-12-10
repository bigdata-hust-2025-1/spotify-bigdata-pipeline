# spark_jobs/batch/silver_to_gold.py
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Lấy cấu hình từ biến môi trường
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio.bigdata:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "miniopass123")

# --- NGÀY DỮ LIỆU ---
INGEST_DATE = "2025-12-06"

SILVER = "s3a://spotify-silver"
GOLD = "s3a://spotify-gold"

# =============================
# Spark Session Setup
# =============================
spark = (
    SparkSession.builder
        .appName("SilverToGold_Analytics")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
)

# ================================================================
# 1. GOLD: ARTISTS STATS
# ================================================================
def build_gold_artists_stats():
    print(f"\n=== GOLD: artists_stats (Date: {INGEST_DATE}) ===")
    
    try:
        df_tracks = spark.read.parquet(f"{SILVER}/tracks/ingest_date={INGEST_DATE}")
        df_artists = spark.read.parquet(f"{SILVER}/artists/ingest_date={INGEST_DATE}")

        # Explode artist_ids
        df_tracks_exploded = df_tracks.withColumn("artist_id", explode(split(col("artist_ids"), ",")))

        # GroupBy và tính toán
        df_stats = (
            df_tracks_exploded.groupBy("artist_id")
                .agg(
                    # [FIXED] Dùng track_id thay vì id
                    count("track_id").alias("track_count"),
                    avg("popularity").alias("avg_track_popularity"),
                    max("popularity").alias("max_track_popularity")
                )
        )

        # Lấy thông tin nghệ sĩ
        df_artists_small = df_artists.select(
            col("artist_id"), 
            col("name").alias("artist_name"),
            "genres",
            col("followers_total"),
            col("popularity").alias("artist_popularity")
        )

        # Join
        df_gold = (
            df_stats.join(df_artists_small, on="artist_id", how="left")
                    .orderBy(col("track_count").desc())
        )

        output_path = f"{GOLD}/artists_stats/ingest_date={INGEST_DATE}"
        df_gold.write.mode("overwrite").parquet(output_path)
        print(f"✅ Saved Artists Stats: {output_path}")
        
    except Exception as e:
        print(f"❌ Error Artists Stats: {str(e)}")


# ================================================================
# 2. GOLD: ALBUMS STATS
# ================================================================
def build_gold_albums_stats():
    print(f"\n=== GOLD: albums_stats (Date: {INGEST_DATE}) ===")

    try:
        df_tracks = spark.read.parquet(f"{SILVER}/tracks/ingest_date={INGEST_DATE}")
        df_albums = spark.read.parquet(f"{SILVER}/albums/ingest_date={INGEST_DATE}")

        # GroupBy và tính toán
        df_stats = (
            df_tracks.groupBy("album_id")
                .agg(
                    # [FIXED] Dùng track_id thay vì id
                    count("track_id").alias("track_count_in_tracks"),
                    avg("popularity").alias("avg_track_popularity"),
                    sum("duration_sec").alias("total_duration_sec")
                )
        )

        # Lấy thông tin album
        df_albums_small = df_albums.select(
            col("album_id"),
            col("name").alias("album_name"),
            "album_type",
            "total_tracks",
            "release_date",
            "artist_ids",
            "genres",
            "label",
            col("popularity").alias("album_popularity")
        )

        # Join
        df_gold = (
            df_stats.join(df_albums_small, on="album_id", how="left")
                    .orderBy(col("track_count_in_tracks").desc())
        )

        output_path = f"{GOLD}/albums_stats/ingest_date={INGEST_DATE}"
        df_gold.write.mode("overwrite").parquet(output_path)
        print(f"✅ Saved Albums Stats: {output_path}")

    except Exception as e:
        print(f"❌ Error Albums Stats: {str(e)}")

# ================================================================
# MAIN
# ================================================================
if __name__ == "__main__":
    build_gold_artists_stats()
    build_gold_albums_stats()
    spark.stop()
    print("\nDONE: Silver → Gold Processing")