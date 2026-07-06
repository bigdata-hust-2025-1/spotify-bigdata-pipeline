# common/

Shared, import-safe helpers used by every batch/stream job. Importing anything
here has **no side effects** (no I/O, no secrets, no JVM) — heavy deps like
`pyspark` are deferred into the functions that need them.

| File | Purpose |
| :--- | :--- |
| `config.py` | Single source of truth for Kafka topic names, bucket names, endpoints, and `get_ingest_date()` / `require_env()` (fail-fast on missing secrets). |
| `spark.py` | `build_spark()` (MinIO S3A + optional Iceberg 3.5 catalog, AQE on) and pure helpers `silver_table()` / `gold_table()` / `build_merge_sql()`. |
| `modeling.py` | Star-schema helpers — `star_table()` naming for the `gold_star` namespace. |
| `logging.py` | Structured JSON logger (`get_logger`), `stage_timer` context manager, and `FailureCollector` for per-dataset isolation with a non-zero exit on failure. |

These are the seams the rest of the repo builds on; change a topic/bucket/endpoint
here, not in a job.
