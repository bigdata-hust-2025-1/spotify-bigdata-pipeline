"""Build the per-user feature table for anomaly training (PR-16).

Closes the MLOps gap (findings D3, C6): the anomaly trainer expected a
``play_duration`` / ``skip_count`` table that **no job produced**. This job
derives those features from the star-schema fact ``gold_star.fact_playback``
(PR-09) — the single event-grain source of truth — aggregating one row per
``user_id``:

======================  =========================================================
column                  meaning
======================  =========================================================
``user_id``             business key (feature-store grain)
``play_count``          number of playback events
``play_duration``       **model input** — mean listen duration (ms) per user
``total_play_duration_ms``  sum of listen duration (ms)
``skip_count``          **model input** — number of skipped plays
``skip_rate``           ``skip_count / play_count``
``distinct_tracks``     distinct tracks the user played
``as_of_date``          logical run date (feature snapshot date)
======================  =========================================================

The result is written two ways, sharing one env-configured path so the loop is
end-to-end runnable:

* **Iceberg** ``lakehouse.gold.user_features`` — the lakehouse source of truth,
  ``MERGE``-upserted on ``user_id`` so re-running a date is idempotent.
* **Parquet export** at ``FEATURES_PATH`` — a flat snapshot the lightweight
  sklearn trainer (``mlops/train_anomaly_model.py``) reads with pandas, without
  needing an Iceberg catalog in the training container.

Design mirrors the other batch jobs: the Spark session comes from
``common.spark.build_spark`` (Iceberg runtime + MinIO S3A), logging is the
structured JSON logger from PR-13, and failures raise loudly (non-zero exit).
The feature-name contract (:data:`TRAINING_FEATURES`) is a pure module constant
so the trainer and its tests import it without a JVM.
"""

import os
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.config import GOLD_BUCKET, get_ingest_date  # noqa: E402
from common.logging import get_logger, stage_timer  # noqa: E402
from common.modeling import star_table  # noqa: E402
from common.spark import build_merge_sql, build_spark, gold_table  # noqa: E402

LOG = get_logger("build_features")

# Logical snapshot date (override via INGEST_DATE / Airflow {{ ds }}).
AS_OF_DATE = get_ingest_date()

# The two model input features, in the exact order the trainer expects. Kept here
# (pure constant, no Spark) so ``mlops/train_anomaly_model.py`` and its JVM-free
# tests share one contract with this producer.
TRAINING_FEATURES = ["play_duration", "skip_count"]

# Feature-store table (Iceberg) + flat Parquet export the trainer consumes. Both
# default under the Gold bucket; override the export path with FEATURES_PATH.
FEATURE_TABLE = "user_features"
FEATURES_PATH = os.getenv(
    "FEATURES_PATH", f"s3a://{GOLD_BUCKET}/features/user_features"
)

# Business key of the feature store (one row per user) — drives the MERGE.
FEATURE_KEY = ["user_id"]


def get_spark_session():
    return build_spark("Spotify_Build_Features", iceberg=True)


def aggregate_user_features(fact_df):
    """Aggregate ``fact_playback`` rows into one feature row per user.

    Pure Spark transform (no I/O): given the event-grain fact DataFrame it
    returns the per-user feature DataFrame whose columns match the feature-store
    schema documented in the module docstring. ``pyspark`` is imported lazily so
    importing this module needs no JVM.
    """
    from pyspark.sql import functions as F  # deferred

    return (
        fact_df.groupBy("user_id")
        .agg(
            F.count(F.lit(1)).alias("play_count"),
            F.avg("listen_duration_ms").alias("play_duration"),
            F.sum("listen_duration_ms").alias("total_play_duration_ms"),
            F.sum("is_skipped").alias("skip_count"),
            F.countDistinct("track_sk").alias("distinct_tracks"),
        )
        .withColumn(
            "skip_rate",
            F.when(
                F.col("play_count") > 0,
                F.col("skip_count") / F.col("play_count"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("as_of_date", F.lit(AS_OF_DATE))
    )


def write_iceberg(spark, features_df):
    """Upsert the feature snapshot into the Iceberg feature-store table on ``user_id``."""
    table = gold_table(FEATURE_TABLE)
    source_view = "_src_user_features"
    features_df.createOrReplaceTempView(source_view)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} USING iceberg "
        f"AS SELECT * FROM {source_view} WHERE 1=0"
    )
    spark.sql(build_merge_sql(table, source_view, FEATURE_KEY))
    LOG.info("iceberg_merge", extra={"stage": FEATURE_TABLE, "table": table})


def write_parquet_export(features_df):
    """Write the flat Parquet snapshot the sklearn trainer reads with pandas."""
    features_df.write.mode("overwrite").parquet(FEATURES_PATH)
    LOG.info("parquet_export", extra={"stage": FEATURE_TABLE, "path": FEATURES_PATH})


def build_features(spark):
    """Read the fact, aggregate features, and publish both outputs. Fails loudly."""
    source = star_table("fact_playback")
    with stage_timer(LOG, FEATURE_TABLE, source=source, as_of=AS_OF_DATE) as m:
        fact_df = spark.table(source)
        features_df = aggregate_user_features(fact_df).cache()
        m["rows_out"] = features_df.count()
        write_iceberg(spark, features_df)
        write_parquet_export(features_df)


def main():
    LOG.info(
        "job_start",
        extra={"job": "build_features", "date": AS_OF_DATE, "path": FEATURES_PATH},
    )
    spark = get_spark_session()
    try:
        build_features(spark)
    finally:
        spark.stop()
    LOG.info("job_done", extra={"job": "build_features"})


if __name__ == "__main__":
    main()
