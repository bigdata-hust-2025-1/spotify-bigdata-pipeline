# Orchestration — `spotify_batch_pipeline` (Spark on Kubernetes)

The batch lakehouse pipeline is orchestrated by the Airflow DAG
`dags/spotify_batch_pipeline.py`. It exists to fix two structural problems the
architecture review flagged:

- **E3 — heavy work in the worker:** the legacy `spotify_pipeline` ran the crawl
  and the MinIO upload *inside the Airflow worker process*. Here every heavy step
  runs in its own Kubernetes pod; the worker only submits and waits.
- **F2 — orphaned Spark ETL:** the Spark batch jobs had no schedule. This DAG
  submits each of them as a `SparkApplication` and blocks on its driver.

## Flow

```
crawl_spotify (pod) → land_bronze (pod)
  → bronze_to_silver  (SparkApplication + sensor)
  → silver_to_gold    (SparkApplication + sensor)
  → gold_to_es        (SparkApplication + sensor)
  → maintenance       (SparkApplication + sensor)
```

Each Spark step is a pair of tasks: `submit_<step>` (a `SparkKubernetesOperator`
that creates the `SparkApplication`) and `wait_<step>` (a
`SparkKubernetesSensor` that pokes until the driver terminates and mirrors its
success/failure as the task's state). The next step is only submitted after the
previous sensor succeeds.

## Date handling & backfills

- The run's logical date `{{ ds }}` flows into every pod and every
  `SparkApplication` (as the `DT` env var).
- Each `SparkApplication` is named `…-{{ ds_nodash }}` so a backfill of several
  dates never collides on the fixed Kubernetes resource name.
- `catchup=True` + `max_active_runs=1`: a re-enabled DAG (or
  `airflow dags backfill`) replays missed dates one at a time. Every step is
  idempotent (Iceberg `MERGE` / `createOrReplace` from PR-07/08), so a re-run or
  backfill of a date produces correct, **non-duplicated** data.

## Prerequisites (cluster)

| Requirement | Detail |
| :--- | :--- |
| Spark Operator | `sparkoperator.k8s.io/v1beta2` installed in the `bigdata` namespace |
| Service account | `spark-operator-spark` (driver `serviceAccount`) |
| `minio-credentials` Secret | keys `access-key` / `secret-key`; referenced via `secretKeyRef` by every `SparkApplication` (no credentials are hardcoded in the YAML anymore) |
| `SPARK_YAML_DIR` | scheduler-visible path to `spark_jobs/batch/yaml/` (default `/opt/airflow/dags/repo/spark_jobs/batch/yaml`) |
| Airflow Variables | `KAFKA_BOOTSTRAP_SERVERS`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET` |

Create the MinIO secret once (values from your rotated credentials — never commit
them):

```bash
kubectl -n bigdata create secret generic minio-credentials \
  --from-literal=access-key="$MINIO_ACCESS_KEY" \
  --from-literal=secret-key="$MINIO_SECRET_KEY"
```

## Validation

- **Unit / spec (runs anywhere):** `python tests/test_batch_pipeline.py` renders
  every `SparkApplication` spec and asserts the per-run name, the `DT` logical
  date in driver+executor, and `secretKeyRef` credentials (no plaintext secret).
- **DAG import (CI):** `test_batch_dag_structure` loads the DAG via `DagBag`,
  asserts no import errors, `catchup=True`, `max_active_runs=1`,
  `depends_on_past=False`, and the full submit/wait task chain.
- **Integration (minikube):** deploy the Spark Operator + `minio-credentials`,
  trigger one run, and confirm each `SparkApplication` driver reaches
  `COMPLETED` and the task turns green; then `airflow dags backfill` two dates and
  confirm the Iceberg tables are not duplicated. Not runnable without a cluster.

## Rollback

`spotify_batch_pipeline` is a **separate** `dag_id`; the legacy `spotify_pipeline`
stays until an explicit cutover. Reverting is deleting this DAG file — there is
no shared state.
