import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, expr, lit, when
from pyspark.sql.types import StructType, ArrayType

# Lấy cấu hình từ biến môi trường
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://host.docker.internal:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "miniopass123")

BRONZE_BUCKET = "s3a://spotify-bronze"
SILVER_BUCKET = "s3a://spotify-silver"
INGEST_DATE = "2025-12-21" # Cập nhật ngày đúng của bạn

def get_spark_session():
    print(f"Connecting to MinIO at: {MINIO_ENDPOINT}")
    return SparkSession.builder \
        .appName("Spotify_Bronze_To_Silver") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def process_dataset(spark, dataset_name, transform_func):
    print(f"\n=== PROCESSING: {dataset_name} ===")
    
    # Trỏ vào thư mục chứa file
    input_path = f"{BRONZE_BUCKET}/{dataset_name}/ingest_date={INGEST_DATE}"
    output_path = f"{SILVER_BUCKET}/{dataset_name}/ingest_date={INGEST_DATE}"
    
    try:
        print(f"--> Reading directory: {input_path}")
        df = spark.read.option("multiline", "true").json(input_path)
        
        count = df.count()
        print(f"    -> Found {count} records.")
        if count == 0:
            print("    [WARN] Dataframe is empty, skipping.")
            return

        # IN SCHEMA ĐỂ DEBUG LỖI
        print(f"--- Schema of {dataset_name} ---")
        df.printSchema()
        # ------------------------------------

        df_clean = transform_func(df)
        
        print(f"--> Writing to: {output_path}")
        df_clean.write.mode("overwrite").parquet(output_path)
        print(f"    [OK] {dataset_name} processed successfully.")
        
    except Exception as e:
        print(f"!!! ERROR processing {dataset_name}: {str(e)}")

# --- CÁC HÀM TRANSFORMATION ---

def transform_tracks(df):
    return df.select(
        col("id").alias("track_id"),
        col("name"),
        col("album.id").alias("album_id"),
        concat_ws(",", col("artists.id")).alias("artist_ids"),
        col("disc_number"),
        col("track_number"),
        col("duration_ms"),
        (col("duration_ms") / 1000).alias("duration_sec"),
        col("explicit"),
        col("popularity"),
        col("release_date"),
        col("is_local"),
        col("type")
    )

def transform_albums(df):
    return df.select(
        col("id").alias("album_id"),
        col("name"),
        col("album_type"),
        col("total_tracks"),
        col("release_date"),
        col("type"),
        concat_ws(",", col("artists.id")).alias("artist_ids"),
        concat_ws(",", col("genres")).alias("genres"),
        col("label"),
        col("popularity"),
        expr("filter(copyrights, x -> x.type = 'C')[0].text").alias("copyright_text_C"),
        expr("filter(copyrights, x -> x.type = 'P')[0].text").alias("copyright_text_P")
    )

def transform_artists(df):
    return df.select(
        col("id").alias("artist_id"),
        col("name"),
        concat_ws(",", col("genres")).alias("genres"),
        col("followers.total").alias("followers_total"),
        col("popularity"),
        col("type")
    )

def transform_owners(df):
    return df.select(
        col("id").alias("owner_id"),
        col("display_name"),
        col("type")
    )

def transform_playlists(df):
    cols = df.columns
    select_exprs = [
        col("id").alias("playlist_id"),
        col("name"),
        col("description"),
        col("collaborative"),
        col("public"),
        col("snapshot_id"),
        col("type")
    ]

    # Xử lý an toàn cho followers.total
    if "followers" in cols:
        # Chúng ta thử access, nếu lỗi thì điền null
        select_exprs.append(col("followers.total").alias("followers_total"))
    else:
        select_exprs.append(lit(None).alias("followers_total"))

    # Xử lý an toàn cho owner
    if "owner" in cols:
        select_exprs.append(col("owner.id").alias("owner_id"))
        select_exprs.append(col("owner.display_name").alias("owner_display_name"))
        select_exprs.append(col("owner.type").alias("owner_type"))
    else:
        select_exprs.append(lit(None).alias("owner_id"))
        select_exprs.append(lit(None).alias("owner_display_name"))
        select_exprs.append(lit(None).alias("owner_type"))

    # Xử lý an toàn cho primary_color
    if "primary_color" in cols:
        select_exprs.append(col("primary_color"))
    else:
        select_exprs.append(lit(None).cast("string").alias("primary_color"))

    return df.select(*select_exprs)

if __name__ == "__main__":
    spark = get_spark_session()
    
    process_dataset(spark, "tracks", transform_tracks)
    process_dataset(spark, "albums", transform_albums)
    process_dataset(spark, "artists", transform_artists)
    process_dataset(spark, "owners", transform_owners)
    process_dataset(spark, "playlists", transform_playlists)
    
    spark.stop()
    print("\n>>> ALL JOBS FINISHED <<<")