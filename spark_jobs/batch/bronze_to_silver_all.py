import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat_ws, expr, lit

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.config import BRONZE_BUCKET, SILVER_BUCKET, get_ingest_date  # noqa: E402
from common.logging import (  # noqa: E402
    FailureCollector,
    get_logger,
    stage_timer,
)
from common.spark import build_merge_sql, build_spark, silver_table  # noqa: E402
from spark_jobs.quality.checks import key_checks  # noqa: E402

LOG = get_logger("bronze_to_silver")

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


# --- Duration categorisation (Phân loại độ dài bài hát) ---
# Thresholds in milliseconds (single source of truth, shared by the pure
# reference below and the native expression). <3 min = Short, 3–5 min = Medium.
DURATION_SHORT_MS = 180_000   # 3 minutes
DURATION_MEDIUM_MS = 300_000  # 5 minutes


def categorize_duration(ms):
    """Reference categorisation — pure Python, the single source of truth for
    the boundary semantics (unit-tested in CI with no JVM).

    The hot path uses :func:`duration_category_expr`, a native Spark expression
    that mirrors this exactly; keeping this pure function lets the boundaries be
    verified without a Spark runtime.
    """
    if not ms:                       # None or 0 ms
        return "Unknown"
    if ms < DURATION_SHORT_MS:
        return "Short"
    if ms < DURATION_MEDIUM_MS:
        return "Medium"
    return "Long"


def duration_category_expr(ms_col="duration_ms"):
    """Native (codegen) equivalent of :func:`categorize_duration`.

    Replaces the former Python UDF (finding C4): categorisation now runs inside
    the JVM with no per-row Python (de)serialisation across the boundary, which
    is the expensive part of a UDF in the hot path. Returns a
    :class:`~pyspark.sql.Column` built from the same thresholds.
    """
    ms = F.col(ms_col)
    return (
        F.when(ms.isNull() | (ms == 0), "Unknown")
        .when(ms < DURATION_SHORT_MS, "Short")
        .when(ms < DURATION_MEDIUM_MS, "Medium")
        .otherwise("Long")
    )
# ----------------------------------------------------------


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
        # Target ~128 MiB data files on write so MERGE/streaming don't accumulate
        # tiny files (finding J2); complements the compaction job (PR-08).
        "TBLPROPERTIES ('write.target-file-size-bytes'='134217728') "
        f"AS SELECT * FROM {source_view} WHERE 1=0"
    )
    spark.sql(build_merge_sql(table, source_view, key_columns))
    LOG.info("iceberg_merge", extra={"stage": dataset_name, "table": table})


def write_parquet(dataset_name, df_clean):
    """Legacy behaviour: overwrite the date-partitioned Parquet directory."""
    output_path = f"{SILVER_PATH}/{dataset_name}/ingest_date={INGEST_DATE}"
    df_clean.write.mode("overwrite").parquet(output_path)
    LOG.info("parquet_write", extra={"stage": dataset_name, "path": output_path})


def process_dataset(spark, dataset_name, transform_func):
    """Read → transform → write one dataset. Raises on failure (fail loudly).

    Per-dataset isolation is handled by the caller's FailureCollector; this
    function no longer swallows errors, so a genuine failure propagates instead
    of the job silently exiting 0 (findings C5, H1).
    """
    input_path = f"{BRONZE_PATH}/{dataset_name}/ingest_date={INGEST_DATE}"
    with stage_timer(LOG, dataset_name, format=SILVER_FORMAT, input=input_path) as m:
        df = spark.read.option("multiline", "true").json(input_path)
        # Short-circuit an empty date-partition with isEmpty() (stops at the first
        # row) instead of a full count()-as-guard — a common, cheap case during
        # backfills. The full count is only taken below, as a real metric.
        if df.isEmpty():
            m["rows_in"] = 0
            m["skipped"] = True
            LOG.warning("empty_input", extra={"stage": dataset_name, "date": INGEST_DATE})
            return
        m["rows_in"] = df.count()

        df_clean = transform_func(df)
        m["rows_out"] = df_clean.count()

        # Data-quality gate (PR-15): non-null + unique business key. Runs before
        # the write so bad data never lands in Silver; a breach raises
        # DataQualityError (recorded by the caller's FailureCollector → the job
        # exits non-zero) instead of flowing silently to Gold.
        report = key_checks(df_clean, BUSINESS_KEYS[dataset_name], f"silver.{dataset_name}")
        report.log(LOG)
        report.raise_if_failed()

        if SILVER_FORMAT == "iceberg":
            write_iceberg(spark, dataset_name, df_clean, BUSINESS_KEYS[dataset_name])
        else:
            write_parquet(dataset_name, df_clean)


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
        # Native duration categorisation (no Python UDF in the hot path).
        duration_category_expr().alias("duration_category"),
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


DATASETS = [
    ("tracks", transform_tracks),
    ("albums", transform_albums),
    ("artists", transform_artists),
    ("owners", transform_owners),
    ("playlists", transform_playlists),
]


def main():
    LOG.info("job_start", extra={"job": "bronze_to_silver", "date": INGEST_DATE,
                                 "format": SILVER_FORMAT})
    spark = get_spark_session()
    failures = FailureCollector(LOG)
    try:
        for dataset_name, transform_func in DATASETS:
            # Isolate per-dataset failures but keep going; the summary below
            # re-raises so the job exits non-zero if any dataset failed.
            with failures.collect(dataset_name):
                process_dataset(spark, dataset_name, transform_func)
    finally:
        spark.stop()
    failures.raise_if_any()
    LOG.info("job_done", extra={"job": "bronze_to_silver"})


if __name__ == "__main__":
    main()
