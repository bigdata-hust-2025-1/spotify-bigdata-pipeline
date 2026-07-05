"""Build ``fact_playback`` — the event-grain fact of the star schema (PR-09).

Grain: **one row per playback event** (``event_id``). The source is the durable
event store ``spotify_ks.user_plays`` in Cassandra (landed by the streaming sink
in PR-04) — the only place raw play events are retained. Each event is joined to
the *current* version of ``dim_track`` / ``dim_artist`` / ``dim_album`` to pick up
their surrogate keys, and the result is upserted (``MERGE`` on ``event_id``) into
the Iceberg ``gold_star.fact_playback`` table, so re-running is idempotent.

Reading facts from the serving store is a pragmatic choice given the events are
not yet landed to the lake; a Bronze/Silver ``events`` dataset would be the more
standard source (noted in ``docs/DATA_MODEL.md``). The source is selectable via
``FACT_SOURCE`` to make that swap a config change.
"""

import os
import sys

from pyspark.sql import functions as F

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.config import CASSANDRA_HOST, CASSANDRA_PORT  # noqa: E402
from common.modeling import star_table, unresolved_fk_count_sql  # noqa: E402
from common.spark import build_merge_sql, build_spark  # noqa: E402

FACT_SOURCE = os.getenv("FACT_SOURCE", "cassandra").lower()
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "spotify_ks")
CASSANDRA_TABLE = os.getenv("CASSANDRA_USER_PLAYS_TABLE", "user_plays")
CASSANDRA_CONNECTOR = os.getenv(
    "CASSANDRA_SPARK_CONNECTOR",
    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0",
)

# Surrogate-key foreign keys the fact must resolve against its dimensions.
FACT_SK_COLUMNS = ["track_sk", "artist_sk", "album_sk"]


def get_spark_session():
    return build_spark(
        "Spotify_Build_Facts",
        iceberg=True,
        extra_packages=[CASSANDRA_CONNECTOR],
        extra_configs={
            "spark.cassandra.connection.host": CASSANDRA_HOST,
            "spark.cassandra.connection.port": CASSANDRA_PORT,
        },
    )


def read_events(spark):
    """Read raw playback events from the configured source (Cassandra by default)."""
    if FACT_SOURCE == "cassandra":
        return (
            spark.read.format("org.apache.spark.sql.cassandra")
            .options(table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE)
            .load()
        )
    raise ValueError(f"Unsupported FACT_SOURCE '{FACT_SOURCE}' (expected 'cassandra')")


def _current_dim_key(spark, dim_name, natural_key, sk_alias):
    """Return the current (is_current) version's (natural_key, surrogate_key)."""
    return (
        spark.table(star_table(dim_name))
        .filter(F.col("is_current"))
        .select(
            F.col(natural_key),
            F.col("surrogate_key").alias(sk_alias),
        )
    )


def build_fact_playback(spark):
    print("\n=== FACT: fact_playback ===")
    events = read_events(spark)

    dim_track = _current_dim_key(spark, "dim_track", "track_id", "track_sk")
    dim_artist = _current_dim_key(spark, "dim_artist", "artist_id", "artist_sk")
    dim_album = _current_dim_key(spark, "dim_album", "album_id", "album_sk")

    fact = (
        events.join(dim_track, on="track_id", how="left")
        .join(dim_artist, on="artist_id", how="left")
        .join(dim_album, on="album_id", how="left")
        .select(
            # Degenerate dimensions / event identity
            F.col("event_id"),
            F.col("user_id"),
            F.col("event_time"),
            # Foreign keys to the conformed dimensions
            F.col("track_sk"),
            F.col("artist_sk"),
            F.col("album_sk"),
            # Measures
            F.col("listen_duration_ms"),
            F.col("track_popularity"),
            (F.col("status") == F.lit("skipped")).cast("int").alias("is_skipped"),
            # Context
            F.col("status"),
            F.col("location"),
            F.col("device"),
        )
    )

    fact_view = "_src_fact_playback"
    fact.createOrReplaceTempView(fact_view)

    # Referential-integrity check: every FK must resolve to a dimension row.
    unresolved = spark.sql(
        unresolved_fk_count_sql(fact_view, FACT_SK_COLUMNS)
    ).collect()[0]["unresolved"]
    if unresolved:
        print(f"    [WARN] {unresolved} fact rows have unresolved dimension keys")

    table = star_table("fact_playback")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} USING iceberg "
        f"PARTITIONED BY (days(event_time)) "
        f"AS SELECT * FROM {fact_view} WHERE 1=0"
    )
    spark.sql(build_merge_sql(table, fact_view, ["event_id"]))
    print(f"    [OK] MERGE upsert into {table}")


if __name__ == "__main__":
    spark = get_spark_session()
    build_fact_playback(spark)
    spark.stop()
