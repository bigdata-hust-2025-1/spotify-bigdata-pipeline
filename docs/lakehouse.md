# Iceberg Lakehouse — Silver layer (PR-07)

This document describes how the Silver medallion layer is written as Apache
Iceberg tables, and how to operate the feature flag, cutover, and rollback.

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

## Feature flag & cutover

`SILVER_FORMAT` controls the writer:

| Value | Behaviour |
| :--- | :--- |
| `parquet` (**default**) | Legacy `overwrite` Parquet — byte-for-byte the pre-PR-07 behaviour. |
| `iceberg` | Iceberg catalog + MERGE upsert + hidden `days(ingest_ts)` partitioning. |

Default is `parquet` so `main` is behaviour-preserving; the Iceberg tables land
in a **separate namespace/warehouse**, additive to the legacy Parquet output.
A later, explicit cutover PR flips the default and retires the Parquet path.

## Verifying (needs a Spark 3.5 + MinIO environment)

```bash
export SILVER_FORMAT=iceberg
export MINIO_ACCESS_KEY=… MINIO_SECRET_KEY=…
spark-submit spark_jobs/batch/bronze_to_silver_all.py     # run once
spark-submit spark_jobs/batch/bronze_to_silver_all.py     # run again (idempotent)
```

Then in `spark-sql`:

```sql
SELECT * FROM lakehouse.silver.tracks.snapshots;          -- snapshots exist
SELECT count(*) FROM lakehouse.silver.tracks VERSION AS OF <first_snapshot_id>;  -- time travel
```

Expect: a `.snapshots` history, a time-travel query returning a prior snapshot,
and stable row counts across the two runs.

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
- **Gold layer** Iceberg conversion + compaction/snapshot-expiry maintenance is
  PR-08; dimensional star schema is PR-09.
