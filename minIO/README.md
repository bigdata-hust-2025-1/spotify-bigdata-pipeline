# minIO/

> **Legacy / exploratory scripts.** These are the original, pre-Iceberg
> single-file jobs that read and write plain Parquet/CSV directly on MinIO. The
> production medallion ETL now lives in `spark_jobs/` (Iceberg lakehouse) and
> `dags/` (Airflow orchestration). These are kept for reference until the final
> docs cutover (PR-22).

| File | Role |
| :--- | :--- |
| `minio_client.py` | Thin MinIO/S3 client helper. |
| `upload_minio.py` | Upload local data into buckets. |
| `bronze_to_silver.py`, `silver_to_gold.py` | Early medallion transforms (superseded by `spark_jobs/batch/*_all.py`). |
| `get_data_gold.py`, `get_data_sliver.py` | Read helpers (note: `sliver` is a known typo, renamed in PR-21). |

Credentials come from the environment — never hardcode MinIO keys.
