# Configuration & the `common.config` module

`common/config.py` is the **single source of truth** for values that were
previously copy-pasted (and had diverged) across jobs — most importantly the
Kafka **topic taxonomy**. It replaces per-file literals so that producers and
consumers can no longer drift apart (architecture review, findings **A2/B3**).

## Why this exists

The producer wrote `spotify_playback_events`, but the Cassandra consumer
subscribed to `spotify-gold-events` and the Flink job to `spotify-user-events`.
Two of three real-time paths were therefore **dead** — nobody produced the
topics they consumed. Centralising the names removes that entire class of bug.

## Design contract

1. **Everything is environment-overridable.** The same code runs unchanged on a
   laptop, in Docker and in Kubernetes; only the environment differs.
2. **No secrets live here.** Passwords / access keys are intentionally absent;
   they are resolved via a fail-fast `require_env` helper added in **PR-06**.
3. **Import has no side effects** — no network, no filesystem, no client
   construction. Safe to import from any Spark job, Flink shim or Airflow DAG.
   (Enforced by a subprocess test in `tests/test_config.py`.)

## What it defines

| Group | Constants |
| :--- | :--- |
| Kafka topics | `TOPIC_PLAYBACK`, `TOPIC_ANOMALY`, `TOPIC_ALBUMS`, `TOPIC_TRACKS`, `TOPIC_ARTISTS` |
| Buckets (medallion) | `BRONZE_BUCKET`, `SILVER_BUCKET`, `GOLD_BUCKET` |
| Endpoints (non-secret) | `KAFKA_BOOTSTRAP_SERVERS`, `MINIO_ENDPOINT`, `ES_NODES`, `ES_PORT`, `CASSANDRA_HOST`, `CASSANDRA_PORT` |
| Ingest date | `DEFAULT_INGEST_DATE`, `get_ingest_date()` |

### Environment variables

| Variable | Default | Notes |
| :--- | :--- | :--- |
| `TOPIC_PLAYBACK` | `spotify_playback_events` | Playback-events topic (producer + all stream consumers). |
| `TOPIC_ANOMALY` | `spotify_anomaly_events` | Anomaly-events topic (wired on the Flink side in PR-10). |
| `KAFKA_TOPIC_ALBUMS` / `_TRACKS` / `_ARTISTS` | `spotify_albums` / `_tracks` / `_artists` | Crawl topics (env names match the existing crawler vars). |
| `BRONZE_BUCKET` / `SILVER_BUCKET` / `GOLD_BUCKET` | `spotify-bronze` / `-silver` / `-gold` | Object-store buckets. |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka brokers. |
| `MINIO_ENDPOINT` | `http://minio.bigdata:9000` | MinIO/S3 endpoint. |
| `ES_NODES` / `ES_PORT` | `elasticsearch.bigdata` / `9200` | Elasticsearch. |
| `CASSANDRA_HOST` / `CASSANDRA_PORT` | `cassandra.bigdata` / `9042` | Cassandra. |
| `INGEST_DATE` | `2025-12-21` | Logical ingest date (Airflow can inject `{{ ds }}`). |

## Usage

Jobs add the repo root to `sys.path` (so `common` is importable under
`spark-submit`), then import what they need:

```python
import os, sys
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.config import TOPIC_PLAYBACK, get_ingest_date
```

## Adoption is incremental (by design)

Introducing the module and adopting it everywhere in one commit would be a huge,
hard-to-review blast radius. Following the roadmap's dual-path principle, PR-03
wires only the **topic taxonomy** into the streaming jobs; the rest is adopted
where it is behaviour-preserving and in-scope for later PRs:

| Surface | Adopted in |
| :--- | :--- |
| `TOPIC_PLAYBACK` → producer + 3 stream consumers | **PR-03 (this change)** |
| Endpoint/bucket constants → batch Spark jobs | PR-06 |
| Crawl topics → crawler / Kafka→MinIO consumer | PR-11 |
| `get_ingest_date()` → batch jobs | PR-06 / PR-18 |
| `TOPIC_ANOMALY` → Flink anomaly job | PR-05 / PR-10 |

Until a surface is adopted, its constant is simply the canonical definition;
nothing regresses because the old literals it replaces had the same values.
