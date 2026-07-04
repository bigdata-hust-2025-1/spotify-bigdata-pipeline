import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat_ws, expr, lit, udf
from pyspark.sql.types import StringType

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.config import BRONZE_BUCKET, SILVER_BUCKET, get_ingest_date  # noqa: E402
from common.spark import build_merge_sql, build_spark, silver_table  # noqa: E402

# s3a paths for the medallion buckets (bucket names come from common.config).
BRONZE_PATH = f"s3a://{BRONZE_BUCKET}"
SILVER_PATH = f"s3a://{SILVER_BUCKET}"

# Ngày ingest của dữ liệu cần xử lý (override qua INGEST_DATE, vd Airflow {{ ds }}).
INGEST_DATE = get_ingest_date()

# Output format for the Silver layer. Default ``parquet`` preserves the legacy
# overwrite behaviour so `main` is behaviour-preserving until the explicit
# cutover; set ``SILVER_FORMAT=iceberg`` to write the Iceberg Lakehouse tables
# (catalog + MERGE upsert + hidden days(ingest_ts) partitioning).
SILVER_FORMAT = os.getenv("SILVER_FORMAT", "parquet").lower()

# Business (natural) key per dataset — drives the idempotent Iceberg MERGE.
BUSINESS_KEYS = {
    "tracks": ["track_id"],
    "albums": ["album_id"],
    "artists": ["artist_id"],
    "owners": ["owner_id"],
    "playlists": ["playlist_id"],
}


# --- [ADVANCED] Custom UDF: Phân loại độ dài bài hát ---
# Yêu cầu: Custom UDFs for specific business logic
def categorize_duration(ms):
    if not ms:
        return "Unknown"
    sec = ms / 1000
    if sec < 180:
        return "Short"     # Dưới 3 phút
    elif sec < 300:
        return "Medium"    # 3-5 phút
    else:
        return "Long"      # Trên 5 phút


# Đăng ký UDF với Spark
duration_udf = udf(categorize_duration, StringType())
# -------------------------------------------------------


def get_spark_session():
    # Iceberg runtime + Lakehouse catalog are only needed for the Iceberg path.
    return build_spark(
        "Spotify_Bronze_To_Silver_Advanced",
        iceberg=(SILVER_FORMAT == "iceberg"),
    )


def write_iceberg(spark, dataset_name, df_clean, key_columns):
    """Upsert ``df_clean`` into the Silver Iceberg table for ``dataset_name``.

    Stamps the logical ingest date as a timestamp (so Iceberg can hidden-
    partition on ``days(ingest_ts)``), creates the table on first run with an
    empty CTAS (``WHERE 1=0``) so it inherits the DataFrame schema and
    partitioning, then MERGEs on the business key. Re-running is therefore
    idempotent: matched rows update in place, new rows insert, no duplicates.
    """
    table = silver_table(dataset_name)
    source_view = f"_src_{dataset_name}"
    df_partitioned = df_clean.withColumn("ingest_ts", F.to_timestamp(lit(INGEST_DATE)))
    df_partitioned.createOrReplaceTempView(source_view)

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} "
        "USING iceberg "
        "PARTITIONED BY (days(ingest_ts)) "
        f"AS SELECT * FROM {source_view} WHERE 1=0"
    )
    spark.sql(build_merge_sql(table, source_view, key_columns))
    print(f"    [OK] MERGE upsert into Iceberg table {table}")


def write_parquet(dataset_name, df_clean):
    """Legacy behaviour: overwrite the date-partitioned Parquet directory."""
    output_path = f"{SILVER_PATH}/{dataset_name}/ingest_date={INGEST_DATE}"
    print(f"--> Writing Parquet to: {output_path}")
    df_clean.write.mode("overwrite").parquet(output_path)
    print(f"    [OK] {dataset_name} written as Parquet")


def process_dataset(spark, dataset_name, transform_func):
    print(f"\n=== PROCESSING: {dataset_name} ({SILVER_FORMAT}) ===")
    input_path = f"{BRONZE_PATH}/{dataset_name}/ingest_date={INGEST_DATE}"

    try:
        print(f"--> Reading directory: {input_path}")
        df = spark.read.option("multiline", "true").json(input_path)
        if df.count() == 0:
            print(f"    [SKIP] no {dataset_name} rows for {INGEST_DATE}")
            return

        df_clean = transform_func(df)

        if SILVER_FORMAT == "iceberg":
            write_iceberg(spark, dataset_name, df_clean, BUSINESS_KEYS[dataset_name])
        else:
            write_parquet(dataset_name, df_clean)

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
