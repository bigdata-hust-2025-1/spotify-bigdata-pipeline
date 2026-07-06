# spark_jobs/

PySpark jobs that move data through the medallion architecture. All jobs build
their session via `common.spark.build_spark` and log via `common.logging`.

## `batch/`
The scheduled ETL (orchestrated by the Airflow batch DAG, see `dags/`):

| Job | Role |
| :--- | :--- |
| `bronze_to_silver_all.py` | Raw JSON → Silver (Iceberg `MERGE` upsert or legacy Parquet); native duration categorisation. |
| `silver_to_gold_all.py` | Silver → Gold stats tables (broadcast joins, window functions). |
| `build_dimensions.py` / `build_facts.py` | Star schema — SCD2 dims + `fact_playback` in the `gold_star` namespace. |
| `build_features.py` | Per-user feature table for the MLOps loop (`mlops/`). |
| `gold_to_es.py` | Publish Gold to Elasticsearch for serving. |
| `maintenance.py` | Iceberg compaction / snapshot expiry / orphan-file removal. |
| `advanced_analytics.py`, `get_data_*`, `upload_data.py` | Analytical/loader utilities. |
| `yaml/*.yaml` | `SparkApplication` specs submitted to K8s by the DAG. |

## `stream/`
Structured Streaming jobs (Kafka → Cassandra / Elasticsearch) with durable
checkpoints under `CHECKPOINT_ROOT`.

## `quality/`
Reusable data-quality checks (`checks.py`) run as gates between layers (PR-15).

Format is feature-flagged (`SILVER_FORMAT` / `GOLD_FORMAT` = `parquet|iceberg`)
so the Iceberg path is additive until an explicit cutover.
