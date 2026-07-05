# Changelog

All notable changes to this repository are documented here. Entries are grouped
by the roadmap PR they implement.

## PR-09 — Dimensional model: star schema + SCD2 dimensions

**Type:** feat (modeling) · **Branch:** `pr-009-star-schema` (stacks on PR-08)

### Context
Silver mirrors source JSON (`artist_ids` as a delimited string) and the legacy
Gold `*_stats` tables flatten-then-reaggregate with no conformed dimensions,
surrogate keys, or history (finding B2). PR-09 adds a proper star schema in a new
`gold_star` Iceberg namespace, **additive** to the untouched legacy `*_stats`.

### Changed
- **`common/modeling.py`** (new) — pure, JVM-free helpers: `star_table()`,
  `attribute_hash_expr()` (null-safe SCD2 change detection),
  `surrogate_key_expr()` (deterministic per-version key), the three SCD2
  statements (`scd2_close_sql` / `scd2_insert_sql`), and
  `unresolved_fk_count_sql()` (referential-integrity check).
- **`spark_jobs/batch/build_dimensions.py`** (new) — SCD2 `dim_artist` /
  `dim_album` / `dim_track` from Silver via the idempotent three-step (stage →
  close changed → insert new/changed). `dim_track` splits `artist_ids` into
  `array<string>` so the model has no delimited-string ids.
- **`spark_jobs/batch/build_facts.py`** (new) — `fact_playback` at event grain
  (`event_id`) from Cassandra `spotify_ks.user_plays`, left-joined to the current
  dimension versions for surrogate keys, `MERGE`-upserted into
  `gold_star.fact_playback` (partitioned by `days(event_time)`).
- **`common/spark.py`** — added an `extra_configs` hook to `build_spark` (for the
  `spark.cassandra.connection.*` host/port). Backward-compatible.
- **`tests/test_modeling.py`** (new, 11) — naming, hash/key exprs, SCD2 SQL
  (close/insert/composite key), FK-check + guards.
- **Docs** — `docs/DATA_MODEL.md` (ERD, grain, surrogate keys, SCD2 flow,
  verification).

### Design decisions
1. **Additive star model in `gold_star`, Silver untouched.** The roadmap's
   Silver `artist_ids` string→array change would break shared Silver consumers
   (`minIO/silver_to_gold.py`, PR-08 Gold) and violate "main stays green", so the
   array split lives in `dim_track` — the AC "no delimited-string id columns
   remain **in the model**" holds without a Silver schema change. Silver cutover
   deferred.
2. **Deterministic hash surrogate keys.** `sha2(business_key || valid_from)` is
   reproducible (idempotent load) and unique per version — unlike
   `monotonically_increasing_id`, which would churn keys every run.
3. **SCD2 via a 3-step MERGE/anti-join, not one statement.** A single MERGE
   cannot both close the old version and insert the new one for the same key;
   close-then-anti-join-insert is the standard, idempotent pattern.
4. **Fact from Cassandra (`FACT_SOURCE`).** Raw events are only retained in the
   serving store (PR-04); reading it is pragmatic and the source is configurable
   so a future lake-landed `events` dataset is a config swap.
5. **Pure SQL builders.** All SCD2/key/hash logic is side-effect-free and
   unit-tested with no JVM (mirrors `build_merge_sql`).

### Verification
- `python -m py_compile` on all new/edited files; `ruff (E,F)` clean.
- `python tests/test_modeling.py` (11) + existing suites pass with no Spark/JVM.
- Live SCD2 history / fact FK-resolution need a Spark 3.5 + MinIO + Cassandra
  environment (documented in `docs/DATA_MODEL.md`, not runnable here).

### New environment variables
| Variable | Default | Notes |
| :--- | :--- | :--- |
| `GOLD_STAR_NAMESPACE` | `gold_star` | Iceberg namespace for the star model. |
| `FACT_SOURCE` | `cassandra` | Playback-event source for `fact_playback`. |
| `CASSANDRA_SPARK_CONNECTOR` | `com.datastax.spark:spark-cassandra-connector_2.12:3.5.0` | Batch Cassandra read. |

### Rollback
New namespace/tables are additive; `git revert` removes the builders and helpers,
legacy `*_stats` and Silver are untouched. Drop `gold_star.*` to reclaim storage.

## PR-08 — Iceberg-backed Gold + maintenance (compaction, snapshot expiry)

**Type:** feat (lakehouse) · **Branch:** `pr-008-iceberg-gold` (stacks on PR-07)

### Context
PR-07 made Silver Iceberg but Gold was still `overwrite` Parquet, so the pipeline
was not end-to-end ACID; `MERGE`/streaming writes accumulate small files with no
compaction; and `gold_to_es.py` synced artists only, using a deprecated `/doc`
mapping type (findings C1/C2/J2).

### Changed
- **`common/spark.py`** — added `gold_table()` (mirrors `silver_table()`, new
  `GOLD_NAMESPACE`) and an `extra_packages` hook on `build_spark` so a job can
  declare extra JVM coordinates (e.g. the ES connector) alongside the Iceberg
  runtime without clobbering `spark.jars.packages`. Backward-compatible.
- **`spark_jobs/batch/silver_to_gold_all.py`** — feature-flagged
  (`GOLD_FORMAT=iceberg|parquet`, **default `parquet`**). The Iceberg path reads
  Silver from the catalog and writes Gold Iceberg tables via
  `writeTo(...).createOrReplace()` (idempotent full-recompute → one new
  snapshot). Dropped the pre-write global `orderBy` (query-time concern; the
  album top-track windowing is retained). Converted the star `functions` import
  to explicit `F.` references.
- **`spark_jobs/batch/gold_to_es.py`** — reads Gold the same dual-path way and
  **syncs both** `batch_artists` and `batch_albums` (completes the artists-only
  sync). Typeless indices (drops the deprecated `/doc` type, consistent with
  PR-04). Declares the ES connector via `ES_SPARK_RUNTIME`.
- **`spark_jobs/batch/maintenance.py`** (new) — `rewrite_data_files`,
  `expire_snapshots` (retain `SNAPSHOT_RETAIN_LAST`, default 5), and
  `remove_orphan_files` over the Silver + Gold tables (targets overridable via
  `MAINTENANCE_TABLES`). Pure, JVM-free SQL builders.
- **`tests/test_maintenance.py`** (new, 7) — maintenance SQL builders + retain
  guard + target-set coverage/override. **`tests/test_spark.py`** — `gold_table`
  coverage (+3).
- **Docs** — `docs/lakehouse.md` gains Gold, maintenance, and the `GOLD_FORMAT`
  flag/cutover.

### Design decisions
1. **`GOLD_FORMAT` mirrors `SILVER_FORMAT`, default parquet.** `main` stays
   byte-for-byte behaviour-preserving; the Iceberg Gold tables are additive in a
   separate namespace until an explicit cutover PR.
2. **`createOrReplace` for Gold, not MERGE.** Gold `*_stats` are a full
   aggregate recompute, so a transactional replace is the idempotent primitive
   (stable counts, one new snapshot, history retained). MERGE is for the keyed
   Silver upsert.
3. **Maintenance SQL builders are pure.** The `CALL … system.*` strings are
   generated by side-effect-free functions, so compaction/expiry/orphan logic is
   unit-tested with no JVM (same pattern as `build_merge_sql`).
4. **Typeless ES indices.** ES 7+/8 removed mapping types; `batch_artists/doc`
   would break, so both indices are typeless — consistent with PR-04's stream
   sink.

### Verification
- `python -m py_compile` on every edited/added job; `ruff (E,F)` clean.
- `python tests/test_maintenance.py` (7) + `python tests/test_spark.py` (11) +
  existing config/event-schema suites pass with no Spark/JVM.
- Live Iceberg Gold + compaction/expiry + ES sync need a Spark 3.5 + MinIO + ES
  environment (documented in `docs/lakehouse.md`, not runnable here).

### New environment variables
| Variable | Default | Notes |
| :--- | :--- | :--- |
| `GOLD_FORMAT` | `parquet` | `iceberg` reads Silver Iceberg → writes Gold Iceberg. |
| `GOLD_NAMESPACE` | `gold` | Iceberg namespace for Gold tables. |
| `SNAPSHOT_RETAIN_LAST` | `5` | Snapshots retained by `expire_snapshots`. |
| `MAINTENANCE_TABLES` | *(silver+gold)* | Comma-separated `namespace.table` override. |
| `ES_SPARK_RUNTIME` | `org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4` | ES connector coordinate. |

### Rollback
Config-only: set `GOLD_FORMAT=parquet` to return to the legacy Gold writer/ES
read, or `git revert`. The Iceberg Gold tables are additive (separate
namespace); dropping them does not touch legacy Parquet.

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
