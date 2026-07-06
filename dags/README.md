# dags/

Airflow DAGs orchestrating ingestion and the batch ETL. Clients (Kafka, MinIO)
are built **inside tasks**, never at DAG-parse time, so importing a DAG opens no
connections (PR-11).

| File | Role |
| :--- | :--- |
| `spotify_ingestion_bronze.py` | Crawl Spotify → Kafka → land Bronze on the object store. |
| `spotify_batch_pipeline.py` | Bronze→Silver→Gold→ES + maintenance, submitting Spark to K8s via `SparkKubernetesOperator` and waiting on each step (PR-12). |
| `spotify_pipeline.py` | Legacy combined DAG (kept until cutover). |
| `tasks/crawl_spotify.py`, `tasks/kafka_to_minio.py` | Task callables — no module-level side effects; accept the logical date. |

The logical date flows via `{{ ds }}`; each Spark step is idempotent
(Iceberg `MERGE` / partition overwrite), so backfills don't duplicate data.
