# Changelog

All notable changes to this repository are documented here. Entries are grouped
by the roadmap PR they implement.

## PR-07 — Iceberg-backed Silver (catalog + MERGE upsert)

**Type:** feat (lakehouse) · **Branch:** `pr-007-iceberg-silver` (off `main`)

### Context
The "Lakehouse" claim was unbacked: live Silver was `write.mode("overwrite")`
Parquet, and the only Iceberg script (`bronze_to_silver_iceberg.py`) was
orphaned and pinned the **Spark 3.3** Iceberg runtime against a `pyspark==3.5`
project — a guaranteed jar mismatch (findings C1/C2/C3).

### Changed
- **`common/spark.py`** (new) — central `build_spark(app_name, iceberg=…)` with
  the MinIO S3A config and the **Iceberg 3.5** runtime + a Hadoop `lakehouse`
  catalog whose warehouse lives on MinIO. Plus pure, JVM-free helpers
  `silver_table()` and `build_merge_sql()`.
- **`spark_jobs/batch/bronze_to_silver_all.py`** — feature-flagged
  (`SILVER_FORMAT=iceberg|parquet`, **default `parquet`** so `main` is
  behaviour-preserving until the cutover). The Iceberg path creates each Silver
  table (empty CTAS to inherit schema + hidden `days(ingest_ts)` partitioning)
  then `MERGE INTO` upserts on the business key → idempotent re-runs.
- **`requirements.txt`** — documents the Iceberg Spark runtime coordinate
  aligned to Spark 3.5 (`iceberg-spark-runtime-3.5_2.12`).
- **Deleted** `spark_jobs/batch/bronze_to_silver_iceberg.py` (orphan, Spark 3.3).
- **`tests/test_spark.py`** (new, 8) — table naming, MERGE-SQL generation, empty-
  key guard, runtime-classifier check, per-dataset business-key coverage.
- **Docs** — `docs/lakehouse.md` (catalog, MERGE, partitioning, cutover, rollback).

## PR-06 — Fail-fast credentials + kill hardcoded paths

**Type:** fix (security) · **Branch:** `pr-006-fail-fast-config` (stacks on PR-03)

### Context
Most Spark/minIO jobs baked in insecure MinIO defaults
(`os.getenv("MINIO_SECRET_KEY", "miniopass123")`, or literal `minioadmin`), so a
missing secret silently became a well-known credential; several scripts also
hardcoded machine-specific `D:\...` absolute paths (findings G2/G3).

### Changed
- **`common/config.py`** — add `require_env(name)` (raises `RuntimeError` on
  unset/empty, no insecure fallback) and `DATA_DIR` (repo-relative default,
  override with `DATA_DIR`).
- **Credentials → `require_env`** in `minIO/minio_client.py` and 8
  `spark_jobs/batch/*.py` (`advanced_analytics`, `bronze_to_silver_all`,
  `bronze_to_silver_iceberg`, `get_data_gold_all`, `get_data_silver_all`,
  `gold_to_es`, `silver_to_gold_all`, `upload_data`). No `minioadmin` /
  `miniopass123` defaults remain.
- **Hardcoded paths → portable** — `minIO/get_data_gold.py`,
  `minIO/upload_minio.py`, `spark_jobs/batch/get_data_gold_all.py` now derive
  local dirs from `DATA_DIR` / `GOLD_EXPORT_DIR`; stray `D:\...` mentions in
  comments/print strings removed.
- **`tests/test_config.py`** — `require_env` (set / unset / empty) and `DATA_DIR`.
- **Docs** — `docs/CONFIGURATION.md` (require_env + DATA_DIR); README env vars.

### Design decisions
1. **Fail fast, no insecure default.** `require_env` turns a missing secret into
   an immediate, clear error instead of a silent `minioadmin` fallback.
2. **`DATA_DIR` over per-file absolute paths.** One repo-relative, env-overridable
   base makes the local scripts portable; the paths AC was otherwise unmet
   because `D:\...` literals remained (e.g. `get_data_gold_all.py`).
3. **Endpoints left as-is.** The strict scope here is *credentials* and *paths*;
   centralising non-secret endpoints is the roadmap's separate light-touch work.
4. **`sys.path` bootstrap per job** (repo root via `__file__`) so `common`
   imports under `spark-submit`, mirroring PR-03.

### Verification
- `git grep` → no `minioadmin` / `miniopass123` and no `D:\` literals remain.
- `python -m py_compile` on every edited job; `ruff (E,F)` clean on authored lines.
- `python tests/test_config.py` passes (require_env raises on unset/empty).

### New environment variables
| Variable | Default | Notes |
| :--- | :--- | :--- |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | *(required)* | Now mandatory — jobs fail fast if unset. |
| `DATA_DIR` | `<repo>/data` | Local data I/O base (replaces `D:\...`). |
| `GOLD_EXPORT_DIR` | `<DATA_DIR>/data_gold` | Local gold-export target. |

### Rollback
Revert restores the prior defaults; no state change. After revert, jobs again
fall back to insecure defaults, so prefer setting the env vars over reverting.

## PR-03 — Central config module + unified Kafka topic taxonomy

**Type:** refactor + fix · **Branch:** `pr-003-central-config` (stacks on PR-01)

### Context
Kafka topic names were defined as literals in each job and had **diverged**: the
producer wrote `spotify_playback_events`, but the Cassandra consumer subscribed
to `spotify-gold-events` (and the Flink job to `spotify-user-events`), leaving 2
of 3 real-time paths dead (architecture review, findings A2/B3).

### Changed
- **`common/config.py`, `common/__init__.py`** (new) — single source of truth
  for the Kafka topic taxonomy, medallion bucket names, service endpoints, and
  a `get_ingest_date()` helper. Every value is env-overridable; the module holds
  no secrets and has no import side effects.
- **`spark_jobs/stream/produce_to_kafka.py`**, **`stream_processing.py`**,
  **`stream_processing_mongo.py`**, **`stream_to_cassandra.py`** — import
  `TOPIC_PLAYBACK` from `common.config` instead of hardcoding the topic. This
  also **reconnects the Cassandra consumer**, which previously subscribed to the
  dead `spotify-gold-events` topic.
- **`tests/test_config.py`** (new) — unit tests for the config contract
  (non-empty constants, env-override precedence, `get_ingest_date`, and a
  subprocess test proving import performs no network I/O).
- **`docs/CONFIGURATION.md`** (new) — the module's contract and the incremental
  adoption plan.

### Design decisions
1. **Default topic value kept as `spotify_playback_events`.** The producer and
   the ES/Mongo consumers already agreed on it, so centralising on this value is
   behaviour-preserving; only the dead Cassandra subscription changes. Adopting a
   versioned taxonomy (e.g. `spotify.playback.events.v1`) would require
   coordinated Kafka/Flink/infra changes (PR-05/PR-10/PR-17) and is deliberately
   left as a future, env-only migration.
2. **Only the topic is wired in this PR.** Endpoint/bucket adoption across the
   batch layer is PR-06; crawl-topic adoption is PR-11. The module defines the
   full surface (single source of truth) with documented incremental adoption.
3. **`sys.path` bootstrap in each job.** With no packaging in place yet, jobs add
   the repo root (derived from `__file__`) to `sys.path` so `common` imports
   under `spark-submit` — portable, mirroring PR-01's path handling.

### Verification
- `python -m py_compile` passes for all edited jobs and the new module.
- `python tests/test_config.py` → all unit tests pass (no pytest dependency).
- `git grep` confirms no topic-name literals remain in the four stream jobs.

### New environment variables
| Variable | Default | Used by |
| :--- | :--- | :--- |
| `TOPIC_PLAYBACK` | `spotify_playback_events` | producer + all stream jobs |
| `TOPIC_ANOMALY` | `spotify_anomaly_events` | (Flink, wired in PR-10) |

### Rollback
Revert restores the per-file topic literals; no state change. New module is
additive. (The Cassandra consumer would return to its previously dead topic.)

## PR-01 — Resolve merge conflicts + add conflict-marker CI guard

**Type:** fix + ci · **Branch:** `pr-001-resolve-merge-conflicts`

### Context
Three core pipeline files were committed to `main` containing unresolved Git
merge conflict markers, making them invalid Python and taking the batch and
stream-producer paths offline (architecture design review, finding A1).

### Changed
- **`spark_jobs/batch/bronze_to_silver_all.py`** — resolved the conflict by
  **keeping** the `categorize_duration` UDF branch (it is referenced downstream
  in `transform_tracks`, so dropping it would break the job) and making the
  ingest date configurable: `INGEST_DATE = os.getenv("INGEST_DATE", "2025-12-21")`.
- **`spark_jobs/batch/silver_to_gold_all.py`** — same `INGEST_DATE` resolution.
- **`spark_jobs/stream/produce_to_kafka.py`** — kept the single topic name
  (`spotify_playback_events`) and replaced **both** hardcoded absolute Windows
  paths (`D:\...`) with a portable, repo-relative default derived from
  `__file__`, overridable via the `TRACKS_DATA_PATH` environment variable.
- **`.github/workflows/conflict-guard.yml`** (new) — CI job that fails on any
  unresolved merge conflict marker in a tracked file, on every push to `main`
  and every pull request.

### Design decisions
1. **Default `INGEST_DATE` = `2025-12-21`, not `2025-12-06`.** The two dates came
   from the two conflict sides. `2025-12-21` is already used by the
   non-conflicted batch jobs (`advanced_analytics.py`, `gold_to_es.py`), so
   defaulting to it keeps the batch layer internally consistent for anyone who
   runs it as-is.
2. **Env override instead of a hardcoded literal.** A hardcoded date is the root
   cause of this class of conflict (two people editing the same literal). Making
   it an env var with a sane default is the minimal correct fix and sets up
   PR-11/PR-12, where Airflow will inject the logical date (`{{ ds }}`).
   Full centralized config (a shared `common/config.py`) is intentionally
   deferred to PR-03 to keep this PR atomic.
3. **Portable data path derived from `__file__`.** Neither committed Windows
   path is correct on any other machine; the fix is to resolve `data/tracks.json`
   relative to the repository, not to pick one bad absolute path over the other.
   The deeper credential/config hardening remains scoped to PR-06.
4. **Precise, self-excluding guard regex.** The guard matches exactly seven
   marker characters at the start of a line followed by a space/tab/EOL boundary
   (`^(<{7}|={7}|>{7}|\|{7})([ \t]|$)`). The boundary prevents false positives on
   decorative rules (e.g. a long line of `=`), the `^` anchor ignores in-prose
   mentions, and the workflow file excludes itself so the pattern text cannot
   match its own definition. Includes the diff3 `|||||||` marker for completeness.

### Verification
- `python -m py_compile` passes for all three files.
- Working tree scans clean for conflict markers.
- The guard regex was proven to (a) match a real 3-way conflict probe and
  (b) not match a decorative long-`=` line.

### New environment variables
| Variable | Default | Used by |
| :--- | :--- | :--- |
| `INGEST_DATE` | `2025-12-21` | `bronze_to_silver_all.py`, `silver_to_gold_all.py` |
| `TRACKS_DATA_PATH` | `<repo>/data/tracks.json` | `produce_to_kafka.py` |

### Rollback
Pure code/CI change with no runtime state impact. `git revert` of the PR restores
the previous files (though those never compiled) and removes the guard workflow.
