"""Shared schema for the Spotify playback-event stream.

Both streaming consumers (``stream_processing.py`` → Elasticsearch and
``stream_to_cassandra.py`` → Cassandra) parse the same Kafka payload, so the
schema lives here once instead of being duplicated (and drifting) per job.

The field set matches ``produce_to_kafka.generate_event`` exactly.

Importing this module needs only ``pyspark.sql.types`` (pure Python — no JVM),
so it is safe to import in unit tests without a running Spark cluster.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Canonical schema of a playback event, as produced by the Kafka producer.
PLAYBACK_EVENT_SCHEMA = StructType([
    StructField("track_id", StringType()),
    StructField("track_name", StringType()),
    StructField("artist_id", StringType()),
    StructField("album_id", StringType()),
    StructField("track_popularity", IntegerType()),
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", DoubleType()),        # epoch seconds
    StructField("event_time_str", StringType()),
    StructField("location", StringType()),
    StructField("device", StringType()),
    StructField("listen_duration_ms", IntegerType()),
    StructField("status", StringType()),
])

# Columns written to Cassandra ``spotify_ks.user_plays`` — MUST stay in sync
# with ``cassandra/user_plays.cql`` (verified by tests/test_event_schema.py).
# ``event_time`` is derived at query time from the epoch ``timestamp`` field.
USER_PLAYS_COLUMNS = [
    "user_id",
    "event_time",
    "event_id",
    "track_id",
    "track_name",
    "artist_id",
    "album_id",
    "track_popularity",
    "listen_duration_ms",
    "status",
    "location",
    "device",
]

# Columns in USER_PLAYS_COLUMNS that are computed rather than read from the
# raw event payload.
DERIVED_USER_PLAYS_COLUMNS = {"event_time"}
