import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window  # [ADVANCED] window function

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.config import GOLD_BUCKET, SILVER_BUCKET, get_ingest_date  # noqa: E402
from common.logging import (  # noqa: E402
    FailureCollector,
    get_logger,
    stage_timer,
)
from common.spark import build_spark, gold_table, silver_table  # noqa: E402

LOG = get_logger("silver_to_gold")

# s3a paths for the medallion buckets (bucket names come from common.config).
SILVER_PATH = f"s3a://{SILVER_BUCKET}"
GOLD_PATH = f"s3a://{GOLD_BUCKET}"

# Ngày ingest (override qua INGEST_DATE, vd Airflow {{ ds }}).
INGEST_DATE = get_ingest_date()

# Output format for the Gold layer. Default ``parquet`` preserves the legacy
# overwrite behaviour so `main` is behaviour-preserving until the explicit
# cutover; set ``GOLD_FORMAT=iceberg`` to read Silver Iceberg and write the Gold
# Iceberg Lakehouse tables (end-to-end ACID + snapshots + time travel).
GOLD_FORMAT = os.getenv("GOLD_FORMAT", "parquet").lower()


def get_spark_session():
    # Iceberg runtime + Lakehouse catalog are only needed for the Iceberg path.
    return build_spark(
        "Spotify_Silver_To_Gold_Advanced",
        iceberg=(GOLD_FORMAT == "iceberg"),
    )


def read_silver(spark, dataset):
    """Read a Silver dataset.

    Iceberg path reads the catalog table (the current, MERGE-upserted state of
    the whole dataset); the legacy path reads the date-partitioned Parquet
    directory for ``INGEST_DATE``.
    """
    if GOLD_FORMAT == "iceberg":
        return spark.table(silver_table(dataset))
    return spark.read.parquet(f"{SILVER_PATH}/{dataset}/ingest_date={INGEST_DATE}")


def write_gold(df, dataset):
    """Write a Gold dataset as Iceberg (ACID, snapshot per run) or legacy Parquet.

    The Iceberg write uses ``createOrReplace`` — Gold stats are a full recompute
    of an aggregate, so replacing the table contents transactionally keeps
    re-runs idempotent (stable counts, one new snapshot, prior snapshots retained
    for time travel).
    """
    if GOLD_FORMAT == "iceberg":
        table = gold_table(dataset)
        df.writeTo(table).using("iceberg").createOrReplace()
        LOG.info("gold_write", extra={"stage": dataset, "sink": "iceberg", "table": table})
    else:
        output_path = f"{GOLD_PATH}/{dataset}/ingest_date={INGEST_DATE}"
        df.write.mode("overwrite").parquet(output_path)
        LOG.info("gold_write", extra={"stage": dataset, "sink": "parquet", "path": output_path})


def build_gold_artists_stats(spark):
    """Build the artists_stats Gold table. Raises on failure (fail loudly)."""
    with stage_timer(LOG, "artists_stats", format=GOLD_FORMAT) as m:
        df_tracks = read_silver(spark, "tracks")
        df_artists = read_silver(spark, "artists")

        # --- [ADVANCED] Caching Strategy ---
        # df_tracks is reused below, so cache it to avoid re-reading Silver.
        df_tracks.cache()
        m["tracks_in"] = df_tracks.count()
        # -----------------------------------

        df_tracks_exploded = df_tracks.withColumn(
            "artist_id", F.explode(F.split(F.col("artist_ids"), ","))
        )

        df_stats = df_tracks_exploded.groupBy("artist_id").agg(
            F.count("track_id").alias("track_count"),
            F.avg("popularity").alias("avg_track_popularity"),
            F.max("popularity").alias("max_track_popularity"),
        )

        df_artists_small = df_artists.select(
            F.col("artist_id"),
            F.col("name").alias("artist_name"),
            "genres",
            F.col("followers_total"),
        )

        # --- [ADVANCED] Broadcast Join ---
        # Artists (small) is broadcast to every node holding Tracks (large).
        # The pre-write global orderBy was dropped (PR-08): ranking is a query-
        # time concern and a full sort before write is wasted shuffle.
        df_gold = df_stats.join(
            F.broadcast(df_artists_small), on="artist_id", how="left"
        )
        # ---------------------------------

        m["rows_out"] = df_gold.count()
        write_gold(df_gold, "artists_stats")


def build_gold_albums_stats(spark):
    """Build the albums_stats Gold table. Raises on failure (fail loudly)."""
    with stage_timer(LOG, "albums_stats", format=GOLD_FORMAT) as m:
        df_tracks = read_silver(spark, "tracks")
        df_albums = read_silver(spark, "albums")

        # --- [ADVANCED] Window Function ---
        # Rank tracks within each album by popularity, keep the top track.
        window_spec = Window.partitionBy("album_id").orderBy(F.col("popularity").desc())
        df_top_track = (
            df_tracks.withColumn("rank", F.row_number().over(window_spec))
            .filter(F.col("rank") == 1)
            .select(
                "album_id",
                F.col("name").alias("top_track_name"),
                F.col("popularity").alias("top_track_pop"),
            )
        )
        # ----------------------------------

        df_stats = df_tracks.groupBy("album_id").agg(
            F.count("track_id").alias("total_tracks"),
            F.avg("popularity").alias("avg_pop"),
        )

        df_final = df_stats.join(df_top_track, on="album_id", how="left").join(
            df_albums, on="album_id", how="left"
        )

        m["rows_out"] = df_final.count()
        write_gold(df_final, "albums_stats")


GOLD_STAGES = [
    ("artists_stats", build_gold_artists_stats),
    ("albums_stats", build_gold_albums_stats),
]


def main():
    LOG.info("job_start", extra={"job": "silver_to_gold", "date": INGEST_DATE,
                                 "format": GOLD_FORMAT})
    spark = get_spark_session()
    failures = FailureCollector(LOG)
    try:
        for stage_name, build_fn in GOLD_STAGES:
            with failures.collect(stage_name):
                build_fn(spark)
    finally:
        spark.stop()
    failures.raise_if_any()
    LOG.info("job_done", extra={"job": "silver_to_gold"})


if __name__ == "__main__":
    main()
