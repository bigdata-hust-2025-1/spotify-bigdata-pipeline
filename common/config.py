"""Central configuration — the single source of truth for the pipeline.

This module centralises values that were previously copy-pasted (and had
*diverged*) across jobs:

* **Kafka topic taxonomy** — one canonical name per topic.
* **Object-store bucket names** — the medallion (bronze/silver/gold) layers.
* **Service endpoints** — Kafka / MinIO / Elasticsearch / Cassandra hosts.
* **The logical ingest date** helper.

Design contract (relied on by tests and by every importer):

1. **Everything is env-overridable.** The same code runs unchanged on a laptop,
   in Docker, and in Kubernetes; only the environment differs.
2. **No secrets live here.** Passwords / access keys are intentionally absent;
   they are resolved via a fail-fast ``require_env`` helper added in PR-06.
3. **Importing this module has no side effects** — no network, no filesystem
   access, no client construction. Reading ``os.getenv`` is pure, so this is
   safe to import from any Spark job, Flink shim, or Airflow DAG.

Adoption is intentionally incremental (see ``docs/CONFIGURATION.md``): PR-03
wires the topic taxonomy into the streaming jobs; endpoint/bucket/ingest-date
adoption across the batch layer follows in PR-06 / PR-11 / PR-18.
"""

import os

# ---------------------------------------------------------------------------
# Kafka topic taxonomy
# ---------------------------------------------------------------------------
# Historically the producer wrote ``spotify_playback_events`` while consumers
# subscribed to divergent names (``spotify-gold-events``, ``spotify-user-events``),
# leaving 2 of 3 streaming paths dead (architecture review, findings A2/B3).
# These constants are now the ONLY definition of each topic name.
#
# The default values preserve the existing, working playback-topic name so this
# change is behaviour-preserving; adopting a versioned taxonomy later is a pure
# environment change (e.g. ``TOPIC_PLAYBACK=spotify.playback.events.v1``).
TOPIC_PLAYBACK = os.getenv("TOPIC_PLAYBACK", "spotify_playback_events")
TOPIC_ANOMALY = os.getenv("TOPIC_ANOMALY", "spotify_anomaly_events")

# Ingestion (crawl) topics — canonical definitions. The crawler/consumer adopt
# these in the Airflow-focused change (PR-11); env names match the existing
# ``KAFKA_TOPIC_*`` variables so no runtime behaviour changes when adopted.
TOPIC_ALBUMS = os.getenv("KAFKA_TOPIC_ALBUMS", "spotify_albums")
TOPIC_TRACKS = os.getenv("KAFKA_TOPIC_TRACKS", "spotify_tracks")
TOPIC_ARTISTS = os.getenv("KAFKA_TOPIC_ARTISTS", "spotify_artists")

# ---------------------------------------------------------------------------
# Object-store (MinIO / S3) bucket taxonomy — medallion layers
# ---------------------------------------------------------------------------
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "spotify-bronze")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "spotify-silver")
GOLD_BUCKET = os.getenv("GOLD_BUCKET", "spotify-gold")

# ---------------------------------------------------------------------------
# Service endpoints (non-secret)
# ---------------------------------------------------------------------------
# Defaults target the in-cluster Kubernetes DNS names; override per environment
# (e.g. ``localhost:19092`` / ``localhost:9000`` for laptop access).
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio.bigdata:9000")
ES_NODES = os.getenv("ES_NODES", "elasticsearch.bigdata")
ES_PORT = os.getenv("ES_PORT", "9200")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra.bigdata")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")

# ---------------------------------------------------------------------------
# Structured Streaming checkpoints
# ---------------------------------------------------------------------------
# Root for streaming checkpoints. It MUST be durable, shared storage (object
# store) — never a local ``/tmp`` path, which is lost on restart and breaks
# offset recovery. Each query gets its own subdirectory via
# :func:`checkpoint_location`.
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "s3a://spotify-checkpoints")


def checkpoint_location(job_name: str) -> str:
    """Return the durable checkpoint path for a named streaming query.

    ``job_name`` must be a stable identifier for the query — changing it
    orphans the old checkpoint and restarts the stream from scratch.
    """
    if not job_name:
        raise ValueError("job_name must be a non-empty streaming query identifier")
    return f"{CHECKPOINT_ROOT.rstrip('/')}/{job_name}"


# ---------------------------------------------------------------------------
# Logical ingest date
# ---------------------------------------------------------------------------
# Kept in sync with the batch jobs' default (PR-01). Exposed as a function so
# the value is read at call time — an override (e.g. Airflow's ``{{ ds }}``
# injected as ``INGEST_DATE``) always takes effect.
DEFAULT_INGEST_DATE = "2025-12-21"


def get_ingest_date() -> str:
    """Return the logical ingest date.

    Resolves the ``INGEST_DATE`` environment variable, falling back to
    :data:`DEFAULT_INGEST_DATE` when it is unset.
    """
    return os.getenv("INGEST_DATE", DEFAULT_INGEST_DATE)
