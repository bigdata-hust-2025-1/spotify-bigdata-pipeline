"""Build the conformed SCD2 dimensions of the star schema (PR-09).

Reads the Silver datasets and maintains ``dim_artist`` / ``dim_album`` /
``dim_track`` as Iceberg SCD2 tables in the ``gold_star`` namespace — each with a
deterministic surrogate key, an attribute hash for change detection, and
``valid_from`` / ``valid_to`` / ``is_current`` history columns.

The load is the standard idempotent three-step: build a staging snapshot of the
current run, *close* any changed open row, then *insert* new version rows for new
or changed business keys (see ``common.modeling``). ``dim_track`` splits the
delimited ``artist_ids`` into an ``array<string>`` so the model carries no
delimited-string id columns.

Silver is read Iceberg-or-Parquet via ``SILVER_FORMAT`` (matching the batch
convention); dimensions are always written as Iceberg (SCD2 needs MERGE).
"""

import os
import sys

from pyspark.sql import functions as F

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.config import SILVER_BUCKET, get_ingest_date  # noqa: E402
from common.modeling import (  # noqa: E402
    attribute_hash_expr,
    scd2_close_sql,
    scd2_insert_sql,
    star_table,
    surrogate_key_expr,
)
from common.spark import build_spark, silver_table  # noqa: E402

SILVER_PATH = f"s3a://{SILVER_BUCKET}"
INGEST_DATE = get_ingest_date()
SILVER_FORMAT = os.getenv("SILVER_FORMAT", "parquet").lower()

# Dimension specs: source Silver dataset, business (natural) key, tracked
# attributes (SCD2 change detection), and any delimited columns to split into
# arrays so the model holds no delimited-string ids.
DIMENSIONS = {
    "dim_artist": {
        "source": "artists",
        "business_key": ["artist_id"],
        "attrs": ["name", "genres", "followers_total", "popularity"],
        "array_cols": {},
    },
    "dim_album": {
        "source": "albums",
        "business_key": ["album_id"],
        "attrs": [
            "name", "album_type", "total_tracks", "release_date",
            "label", "popularity",
        ],
        "array_cols": {},
    },
    "dim_track": {
        "source": "tracks",
        "business_key": ["track_id"],
        "attrs": [
            "name", "album_id", "artist_ids", "duration_ms",
            "duration_category", "explicit", "popularity", "release_date",
        ],
        # Split the delimited artist ids into an array — no delimited-string ids.
        "array_cols": {"artist_ids": ","},
    },
}


def read_silver(spark, dataset):
    if SILVER_FORMAT == "iceberg":
        return spark.table(silver_table(dataset))
    return spark.read.parquet(f"{SILVER_PATH}/{dataset}/ingest_date={INGEST_DATE}")


def build_staging(df_src, spec):
    """Project the Silver source into the SCD2 staging shape.

    Column order is fixed — ``surrogate_key, <business key>, <attrs>, attr_hash,
    valid_from, valid_to, is_current`` — so it matches the dimension table for
    the ``INSERT INTO … SELECT s.*``.
    """
    business_key = spec["business_key"]
    attrs = spec["attrs"]
    array_cols = spec["array_cols"]

    # Deduplicate the source to one row per business key (latest wins is moot for
    # a single-date load; distinct guards against source fan-out).
    df = df_src.dropDuplicates(business_key)

    # Split delimited columns into arrays in place.
    for col_name, sep in array_cols.items():
        df = df.withColumn(col_name, F.split(F.col(col_name), sep))

    # History columns first (surrogate key depends on valid_from).
    df = (
        df.withColumn("valid_from", F.to_timestamp(F.lit(INGEST_DATE)))
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("attr_hash", F.expr(attribute_hash_expr(attrs)))
        .withColumn("surrogate_key", F.expr(surrogate_key_expr(business_key)))
    )

    ordered = ["surrogate_key"] + business_key + attrs + [
        "attr_hash", "valid_from", "valid_to", "is_current",
    ]
    return df.select(*ordered)


def build_dimension(spark, dim_name, spec):
    print(f"\n=== DIMENSION: {dim_name} ===")
    table = star_table(dim_name)
    staging_view = f"_stg_{dim_name}"
    business_key = spec["business_key"]

    df_stg = build_staging(read_silver(spark, spec["source"]), spec)
    df_stg.createOrReplaceTempView(staging_view)

    # Create on first run with the staging schema (empty), no-op thereafter.
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} USING iceberg "
        f"AS SELECT * FROM {staging_view} WHERE 1=0"
    )
    # SCD2: close changed current rows, then insert new/changed versions.
    spark.sql(scd2_close_sql(table, staging_view, business_key))
    spark.sql(scd2_insert_sql(table, staging_view, business_key))
    print(f"    [OK] SCD2 load into {table}")


if __name__ == "__main__":
    spark = build_spark("Spotify_Build_Dimensions", iceberg=True)
    for name, dim_spec in DIMENSIONS.items():
        build_dimension(spark, name, dim_spec)
    spark.stop()
