import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, expr, lit, udf
from pyspark.sql.types import StructType, ArrayType, StringType

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.config import require_env  # noqa: E402

# Lấy cấu hình từ biến môi trường
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://host.docker.internal:9000")
ACCESS_KEY = require_env("MINIO_ACCESS_KEY")
SECRET_KEY = require_env("MINIO_SECRET_KEY")

BRONZE_BUCKET = "s3a://spotify-bronze"
SILVER_BUCKET = "s3a://spotify-silver"
# Ngày ingest của dữ liệu cần xử lý. Mặc định 2025-12-21 để đồng bộ với các job
# batch khác (advanced_analytics.py, gold_to_es.py). Có thể override qua biến
# môi trường INGEST_DATE (vd: Airflow truyền {{ ds }}).
INGEST_DATE = os.getenv("INGEST_DATE", "2025-12-21")

# --- [ADVANCED] Custom UDF: Phân loại độ dài bài hát ---
# Yêu cầu: Custom UDFs for specific business logic
def categorize_duration(ms):
    if not ms: return "Unknown"
    sec = ms / 1000
    if sec < 180: return "Short"    # Dưới 3 phút
    elif sec < 300: return "Medium" # 3-5 phút
    else: return "Long"             # Trên 5 phút

# Đăng ký UDF với Spark
duration_udf = udf(categorize_duration, StringType())
# -------------------------------------------------------

def get_spark_session():
    print(f"Connecting to MinIO at: {MINIO_ENDPOINT}")
    return SparkSession.builder \
        .appName("Spotify_Bronze_To_Silver_Advanced") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def process_dataset(spark, dataset_name, transform_func):
    print(f"\n=== PROCESSING: {dataset_name} ===")
    input_path = f"{BRONZE_BUCKET}/{dataset_name}/ingest_date={INGEST_DATE}"
    output_path = f"{SILVER_BUCKET}/{dataset_name}/ingest_date={INGEST_DATE}"
    
    try:
        print(f"--> Reading directory: {input_path}")
        df = spark.read.option("multiline", "true").json(input_path)
        if df.count() == 0: return

        df_clean = transform_func(df)
        
        print(f"--> Writing to: {output_path}")
        df_clean.write.mode("overwrite").parquet(output_path)
        print(f"    [OK] {dataset_name} processed.")
        
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
        # --- [ADVANCED] Áp dụng UDF ---
        duration_udf(col("duration_ms")).alias("duration_category"),
        # ------------------------------
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
    # Xử lý an toàn cho cột thiếu
    select_exprs.append(col("followers.total").alias("followers_total") if "followers" in cols else lit(None).alias("followers_total"))
    
    if "owner" in cols:
        select_exprs.append(col("owner.id").alias("owner_id"))
        select_exprs.append(col("owner.display_name").alias("owner_display_name"))
        select_exprs.append(col("owner.type").alias("owner_type"))
    else:
        select_exprs.append(lit(None).alias("owner_id"))
        select_exprs.append(lit(None).alias("owner_display_name"))
        select_exprs.append(lit(None).alias("owner_type"))

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