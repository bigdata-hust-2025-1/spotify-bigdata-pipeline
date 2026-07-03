import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window # [ADVANCED]

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio.bigdata:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "miniopass123")

# --- NGÀY DỮ LIỆU ---
# Mặc định 2025-12-21 để đồng bộ với các job batch khác; override qua biến
# môi trường INGEST_DATE (vd: Airflow truyền {{ ds }}).
INGEST_DATE = os.getenv("INGEST_DATE", "2025-12-21")

SILVER = "s3a://spotify-silver"
GOLD = "s3a://spotify-gold"

spark = (
    SparkSession.builder.appName("SilverToGold_Advanced")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
)

def build_gold_artists_stats():
    print(f"\n=== GOLD: artists_stats ===")
    try:
        df_tracks = spark.read.parquet(f"{SILVER}/tracks/ingest_date={INGEST_DATE}")
        df_artists = spark.read.parquet(f"{SILVER}/artists/ingest_date={INGEST_DATE}")

        # --- [ADVANCED] Caching Strategy ---
        # Vì df_tracks được dùng nhiều lần, cache vào RAM để tối ưu
        df_tracks.cache() 
        print(f"Cached Tracks count: {df_tracks.count()}")
        # -----------------------------------

        df_tracks_exploded = df_tracks.withColumn("artist_id", explode(split(col("artist_ids"), ",")))

        df_stats = (
            df_tracks_exploded.groupBy("artist_id")
                .agg(
                    count("track_id").alias("track_count"),
                    avg("popularity").alias("avg_track_popularity"),
                    max("popularity").alias("max_track_popularity")
                )
        )

        df_artists_small = df_artists.select(
            col("artist_id"), col("name").alias("artist_name"), "genres", col("followers_total")
        )

        # --- [ADVANCED] Broadcast Join ---
        # Bảng Artists (nhỏ) được broadcast đến tất cả node chứa Tracks (lớn) để join nhanh hơn
        df_gold = (
            df_stats.join(broadcast(df_artists_small), on="artist_id", how="left")
                    .orderBy(col("track_count").desc())
        )
        # ---------------------------------

        output_path = f"{GOLD}/artists_stats/ingest_date={INGEST_DATE}"
        df_gold.write.mode("overwrite").parquet(output_path)
        print(f"✅ Saved Artists Stats")
        
    except Exception as e: print(f"❌ Error Artists: {e}")

def build_gold_albums_stats():
    print(f"\n=== GOLD: albums_stats ===")
    try:
        df_tracks = spark.read.parquet(f"{SILVER}/tracks/ingest_date={INGEST_DATE}")
        df_albums = spark.read.parquet(f"{SILVER}/albums/ingest_date={INGEST_DATE}")

        # --- [ADVANCED] Window Function ---
        # Yêu cầu: Xếp hạng các bài hát trong từng Album theo độ phổ biến
        windowSpec = Window.partitionBy("album_id").orderBy(col("popularity").desc())
        
        # Chỉ lấy bài hát Top 1 của mỗi album (The hit song)
        df_top_track = df_tracks.withColumn("rank", row_number().over(windowSpec)) \
                                .filter(col("rank") == 1) \
                                .select("album_id", col("name").alias("top_track_name"), col("popularity").alias("top_track_pop"))
        # ----------------------------------

        # Aggregation thường
        df_stats = df_tracks.groupBy("album_id").agg(
            count("track_id").alias("total_tracks"),
            avg("popularity").alias("avg_pop")
        )

        # Join với Window Result
        df_final = df_stats.join(df_top_track, on="album_id", how="left") \
                           .join(df_albums, on="album_id", how="left")

        output_path = f"{GOLD}/albums_stats/ingest_date={INGEST_DATE}"
        df_final.write.mode("overwrite").parquet(output_path)
        print(f"✅ Saved Albums Stats")

    except Exception as e: print(f"❌ Error Albums: {e}")

if __name__ == "__main__":
    build_gold_artists_stats()
    build_gold_albums_stats()
    spark.stop()