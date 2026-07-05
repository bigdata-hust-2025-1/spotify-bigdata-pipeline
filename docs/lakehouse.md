# Iceberg Lakehouse — Silver & Gold (PR-07, PR-08)

This document describes how the Silver and Gold medallion layers are written as
Apache Iceberg tables, plus table maintenance, and how to operate the feature
flags, cutover, and rollback.

## Why

The repo claimed a "Lakehouse" but the live Silver writer used
`write.mode("overwrite").parquet(...)` (no ACID, no time travel, non-idempotent),
and the only Iceberg script was orphaned and pinned the **Spark 3.3** Iceberg
runtime against a `pyspark==3.5` project — a jar mismatch that could never run
(findings C1/C2/C3). PR-07 makes Silver genuinely Iceberg on Spark 3.5.

## Components

- **`common/spark.py` → `build_spark(app_name, iceberg=…)`** — the single Spark
  builder. Wires MinIO S3A (credentials via `require_env`, fail-fast) and, when
  `iceberg=True`, registers:
  - the Iceberg runtime `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2`
    (env `ICEBERG_SPARK_RUNTIME`),
  - the Iceberg SQL extensions,
  - a Hadoop catalog `lakehouse` (env `LAKEHOUSE_CATALOG`) whose warehouse is
    `s3a://spotify-lakehouse/warehouse` (env `LAKEHOUSE_WAREHOUSE`).
- **Pure helpers** (no Spark, unit-tested in `tests/test_spark.py`):
  - `silver_table("tracks") → "lakehouse.silver.tracks"`,
  - `build_merge_sql(table, source_view, key_columns)` → an idempotent
    `MERGE INTO … WHEN MATCHED UPDATE SET * WHEN NOT MATCHED INSERT *`.

## How the Silver write works (Iceberg path)

For each dataset the job:

1. Reads Bronze JSON and applies the dataset transform (unchanged).
2. Stamps `ingest_ts` (timestamp from the logical `INGEST_DATE`).
3. `CREATE TABLE IF NOT EXISTS lakehouse.silver.<dataset> USING iceberg
   PARTITIONED BY (days(ingest_ts)) AS SELECT * FROM <src> WHERE 1=0` — creates
   the table on first run with the DataFrame schema and **hidden partitioning**,
   and is a no-op thereafter.
4. `MERGE INTO … ON t.<business_key> = s.<business_key>` — upsert.

Business keys: `tracks→track_id`, `albums→album_id`, `artists→artist_id`,
`owners→owner_id`, `playlists→playlist_id`.

**Idempotency:** re-running the same date updates matched rows in place and
inserts new ones — row counts stay stable, exactly one new snapshot is produced,
and no duplicate keys accumulate.

## Gold layer (PR-08)

The Gold writer (`silver_to_gold_all.py`) mirrors the Silver flag with
`GOLD_FORMAT=iceberg|parquet` (default `parquet`). When `iceberg`:

- It **reads Silver from the Iceberg catalog** (`spark.table(lakehouse.silver.*)`
  — the current, MERGE-upserted state of the whole dataset) instead of a single
  date's Parquet directory.
- It **writes Gold as Iceberg** tables (`lakehouse.gold.artists_stats`,
  `lakehouse.gold.albums_stats`) via `writeTo(...).createOrReplace()`. Gold stats
  are a full aggregate recompute, so a transactional replace keeps re-runs
  idempotent (stable counts, one new snapshot, prior snapshots retained for time
  travel).
- The pre-write **global `orderBy` was dropped** — ranking is a query-time
  concern; a full sort before write is wasted shuffle (the album "top track"
  windowing is retained because it is required logic, not a cosmetic sort).

`gold_to_es.py` reads Gold the same dual-path way and **syncs both** artists
(`batch_artists`) and albums (`batch_albums`) to Elasticsearch — completing the
previously artists-only sync. Indices are typeless (no legacy `/doc` mapping
type), consistent with the streaming ES sink fixed in PR-04. The job declares
the Elasticsearch-Spark connector via `ES_SPARK_RUNTIME` (env-overridable).

## Table maintenance (PR-08)

`spark_jobs/batch/maintenance.py` runs the three standard Iceberg maintenance
procedures over the Silver and Gold tables:

| Procedure | Purpose |
| :--- | :--- |
| `rewrite_data_files` | Compacts the small files that `MERGE`/`createOrReplace`/streaming produce. |
| `expire_snapshots` | Drops snapshots beyond `SNAPSHOT_RETAIN_LAST` (default 5) — a retained window so time-travel/rollback still works. |
| `remove_orphan_files` | Deletes files no live snapshot references. |

The procedures are transactional and read-safe. The target set defaults to all
Silver + Gold tables and is overridable with `MAINTENANCE_TABLES` (comma-
separated `namespace.table`). The job is standalone now; Airflow schedules it in
PR-12.

## Feature flag & cutover

Two flags control the writers (both default to the behaviour-preserving Parquet
path so `main` is byte-for-byte unchanged until an explicit cutover):

| Variable | Value | Behaviour |
| :--- | :--- | :--- |
| `SILVER_FORMAT` | `parquet` (**default**) | Legacy `overwrite` Parquet Silver. |
| `SILVER_FORMAT` | `iceberg` | Iceberg catalog + MERGE upsert + hidden `days(ingest_ts)` partitioning. |
| `GOLD_FORMAT` | `parquet` (**default**) | Legacy `overwrite` Parquet Gold; ES reads Parquet. |
| `GOLD_FORMAT` | `iceberg` | Read Silver Iceberg → write Gold Iceberg; ES reads Gold Iceberg. |

The Iceberg tables land in a **separate namespace/warehouse**, additive to the
legacy Parquet output. A later, explicit cutover PR flips the defaults and
retires the Parquet paths. (Run Silver and Gold in the same mode end-to-end:
`GOLD_FORMAT=iceberg` reads the Silver *Iceberg* tables, so Silver must have been
written with `SILVER_FORMAT=iceberg`.)

## Verifying (needs a Spark 3.5 + MinIO environment)

```bash
export SILVER_FORMAT=iceberg GOLD_FORMAT=iceberg
export MINIO_ACCESS_KEY=… MINIO_SECRET_KEY=…
spark-submit spark_jobs/batch/bronze_to_silver_all.py     # run once
spark-submit spark_jobs/batch/bronze_to_silver_all.py     # run again (idempotent)
spark-submit spark_jobs/batch/silver_to_gold_all.py       # Gold Iceberg
spark-submit spark_jobs/batch/maintenance.py              # compaction + expiry
spark-submit spark_jobs/batch/gold_to_es.py               # both indices -> ES
```

Then in `spark-sql`:

```sql
SELECT * FROM lakehouse.silver.tracks.snapshots;          -- snapshots exist
SELECT count(*) FROM lakehouse.silver.tracks VERSION AS OF <first_snapshot_id>;  -- time travel
SELECT * FROM lakehouse.gold.artists_stats.snapshots;     -- Gold is Iceberg too
```

Expect: a `.snapshots` history on both layers, a time-travel query returning a
prior snapshot, stable row counts across re-runs, a reduced data-file count
after `maintenance.py` on a fragmented table, and both `batch_artists` and
`batch_albums` indices in Elasticsearch.

## Rollback

Set `SILVER_FORMAT=parquet` (config-only) to return to the legacy writer, or
`git revert` the PR. The Iceberg tables are additive (separate namespace), so
dropping them does not touch legacy Parquet. If rolling back after Iceberg data
was written, drop the `lakehouse.silver.*` tables to avoid stale metadata.

## Known limitations / follow-ups

- **Schema evolution:** `CREATE TABLE IF NOT EXISTS` fixes the schema on first
  run; adding a Silver column later needs an `ALTER TABLE … ADD COLUMN` (the
  transforms are explicit selects, so the schema is stable in practice).
- **Error handling:** the per-dataset `try/except` still logs-and-continues
  (inherited); fail-fast/structured logging is PR-13.
- **Gold model:** Gold is now Iceberg (PR-08) but still the legacy `*_stats`
  aggregates. The conformed dimensional star schema (`dim_*`/`fact_playback`,
  SCD2) is PR-09.
- **Maintenance scheduling:** `maintenance.py` is standalone; Airflow schedules
  it in PR-12.
- **Incremental Gold:** the Iceberg path aggregates the whole Silver table each
  run (correct current-state stats); delta-scoped processing is PR-18.
