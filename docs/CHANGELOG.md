# Changelog

All notable changes to this repository are documented here. Entries are grouped
by the roadmap PR they implement.

## PR-04 — Reconnect streaming paths + durable checkpoints + Cassandra schema fix

**Type:** fix · **Branch:** `pr-004-streaming-reconnect` (stacks on PR-03)

### Context
The streaming sinks were unreliable (findings A4, D2, F1): `stream_to_cassandra`
wrote **raw Kafka bytes** into the typed `spotify_ks.user_plays` table with no
checkpoint and a mismatched output mode; the Elasticsearch stream checkpointed to
`/tmp` (lost on restart) and used the deprecated `realtime_events/doc` mapping
type. Without a durable checkpoint, neither query could resume from committed
offsets.

### Changed
- **`common/config.py`** — add `CHECKPOINT_ROOT` (durable object-store default,
  never `/tmp`) and a pure `checkpoint_location(job_name)` helper.
- **`spark_jobs/stream/event_schema.py`** (new) — shared `PLAYBACK_EVENT_SCHEMA`
  and `USER_PLAYS_COLUMNS` so both consumers parse one canonical schema.
- **`spark_jobs/stream/stream_to_cassandra.py`** — `from_json` into the typed
  `user_plays` columns; derive `event_time`; event-time watermark +
  `dropDuplicatesWithinWatermark(["event_id"])`; durable checkpoint; output mode
  `append`; restructured into a testable `transform()` + `main()`.
- **`spark_jobs/stream/stream_processing.py`** — shared schema; watermark + dedup;
  typeless `es.resource = "realtime_events"`; `es.mapping.id = event_id` for
  idempotent upserts; durable checkpoint.
- **`cassandra/user_plays.cql`** (new) — the concrete table contract the Spark
  writer targets (kept in sync with `USER_PLAYS_COLUMNS` by tests).
- **`tests/test_event_schema.py`** (new) + `tests/test_config.py` — schema/DDL
  consistency and `checkpoint_location` behaviour.
- **`docs/streaming.md`** (new) — offset-reset policy, delivery semantics, RPO/RTO.

### Design decisions
1. **Durable, per-query checkpoints via a helper.** Centralising the root and
   deriving `<root>/<job>` prevents the `/tmp` mistake and keeps query state
   isolated. Changing a `job_name` intentionally orphans the old checkpoint.
2. **`dropDuplicatesWithinWatermark(["event_id"])`** (Spark 3.5) bounds dedup
   state by the watermark and keys on `event_id` alone — the correct
   state-bounded dedup, versus an unbounded `dropDuplicates`.
3. **Idempotent sinks.** ES keys documents by `event_id`; Cassandra's primary
   key `((user_id), event_time, event_id)` upserts replays in place. Combined
   with at-least-once Kafka delivery this yields effectively-once results.
4. **Shipped the CQL DDL.** The `user_plays` schema existed nowhere in the repo,
   so "columns match" was unverifiable; the DDL makes the contract concrete and
   test-enforced. Scoped strictly to the streaming write; broader modelling
   (star schema/SCD2) remains PR-09.

### Verification
- `py_compile` + `ruff (E,F)` clean on all changed files.
- `python tests/test_config.py` and `tests/test_event_schema.py` pass (no JVM).
- `git grep` confirms both jobs subscribe `TOPIC_PLAYBACK`, no `/tmp` checkpoint
  remains, and no `.../doc` ES type remains.
- Full offset-continuity on kill/restart requires a live Kafka+Cassandra+ES
  cluster (documented runbook in `docs/streaming.md`).

### Rollback
Revert restores the prior files. **Also delete the new checkpoint directories**
(`$CHECKPOINT_ROOT/stream_processing_es`, `.../stream_to_cassandra`) so the old
code does not resume against a checkpoint written by the new schema.

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
