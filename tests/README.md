# tests/

Fast, JVM-free unit tests run by CI (`pytest`). They import `pyspark` where
needed but do **not** start a Spark session — tests that require a runtime
(Spark/JVM, Airflow, MinIO) `raise unittest.SkipTest` and are skipped, so the
suite is green on a laptop with no cluster.

Each test file is also runnable directly (`python tests/test_x.py`) via a small
`_run_all()` harness that recognises the same skips.

| File | Covers |
| :--- | :--- |
| `test_config.py` | Topic/bucket constants, env overrides, no import side effects. |
| `test_spark.py` | Pure Iceberg helpers (table naming, `MERGE` SQL). |
| `test_transforms.py` | Native duration categorisation vs the pure reference. |
| `test_modeling.py` | Star-schema helpers / SCD2 logic. |
| `test_quality.py` | Data-quality check pass/fail fixtures. |
| `test_logging.py` | Structured logger + `stage_timer` fields. |
| `test_event_schema.py` | Event schema (pure `pyspark.sql.types`). |
| `test_dags.py`, `test_batch_pipeline.py` | DAG import + `SparkApplication` specs (skip without Airflow). |
| `test_maintenance.py`, `test_mlops.py` | Maintenance procedures; MLOps training core. |
