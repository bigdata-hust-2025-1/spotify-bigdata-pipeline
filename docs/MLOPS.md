# MLOps — Anomaly-detection training loop

Closes findings **D3 / C6**: the trainer previously read
`s3a://datalake/gold/user_behaviors.parquet`, a path **no job wrote**, so it
could never run against pipeline data. The loop is now connected end-to-end.

## Lifecycle

```
gold_star.fact_playback        (PR-09, event grain: one row per playback event)
        │
        ▼  aggregate per user_id
spark_jobs/batch/build_features.py
        │   ├─► lakehouse.gold.user_features   (Iceberg, MERGE-upsert on user_id)
        │   └─► FEATURES_PATH (Parquet export)  ── training handoff ──┐
        ▼                                                             │
mlops/train_anomaly_model.py  ◄───────────────────────────────────────┘
        │   IsolationForest(contamination, random_state=42)
        ▼
   MLflow Tracking + Model Registry
        │   log params/metrics, log_model, register as ANOMALY_MODEL_NAME
        ▼
   version transitioned to "Staging"
```

## Feature table

`build_features.py` aggregates `fact_playback` to **one row per `user_id`**:

| column | meaning |
| :--- | :--- |
| `user_id` | feature-store business key (MERGE key) |
| `play_count` | number of playback events |
| `play_duration` | **model input** — mean listen duration (ms) |
| `total_play_duration_ms` | sum of listen duration (ms) |
| `skip_count` | **model input** — number of skipped plays |
| `skip_rate` | `skip_count / play_count` |
| `distinct_tracks` | distinct tracks played |
| `as_of_date` | logical snapshot date (`INGEST_DATE` / `{{ ds }}`) |

It writes both an **Iceberg** table (`lakehouse.gold.user_features`, the
lakehouse source of truth, idempotent via `MERGE INTO … ON user_id`) and a flat
**Parquet** export at `FEATURES_PATH`, so the lightweight sklearn trainer reads
features with pandas without needing an Iceberg catalog in its container.

The model-input contract (`play_duration`, `skip_count`) lives in exactly one
place per side — `build_features.TRAINING_FEATURES` and
`train_anomaly_model.FEATURE_COLUMNS` — kept in sync deliberately so the trainer
carries no `pyspark` dependency.

## Training

`train_anomaly_model.py` splits into a pure core and an MLflow wrapper:

- **`train_isolation_forest(pdf)`** — validates the feature contract (missing
  column or empty input **raises**, so a broken feature table fails loudly),
  fits `IsolationForest(contamination, random_state=42)`, returns the model.
  No Spark, no MLflow — fully unit-tested.
- **`run_training()`** — loads the Parquet snapshot, trains, logs
  params (`contamination`, feature/row counts) and metrics
  (`anomaly_count`, `anomaly_rate`), `log_model`s with
  `registered_model_name=ANOMALY_MODEL_NAME`, and transitions the new registry
  version to `Staging`.

## Configuration

| env var | default | purpose |
| :--- | :--- | :--- |
| `FEATURES_PATH` | `s3a://spotify-gold/features/user_features` | Parquet handoff (both jobs) |
| `ANOMALY_MODEL_NAME` | `spotify_anomaly_isolation_forest` | registered model name |
| `ANOMALY_CONTAMINATION` | `0.01` | expected anomaly proportion |
| `MLFLOW_TRACKING_URI` | `http://mlflow-server.bigdata:5000` | MLflow server |
| `MLFLOW_MODEL_STAGE` | `Staging` | registry stage to promote to |
| `INGEST_DATE` | `common.config` default | feature snapshot date |

## Running

```bash
# 1. produce the feature table (needs Spark + Iceberg + the fact table)
INGEST_DATE=2025-12-21 spark-submit spark_jobs/batch/build_features.py

# 2. train + register (needs MLflow + the Parquet export from step 1)
python mlops/train_anomaly_model.py
```

Sequence it after `build_facts.py` in the batch DAG (PR-12); a dedicated
`SparkKubernetesOperator` step for `build_features.py` is the natural follow-up.

## Validation

- **Unit (runs anywhere):** `python tests/test_mlops.py` — the feature contract
  holds, a planted outlier is flagged `-1`, and a missing column / empty table
  each raise `ValueError`. Deterministic via the fixed `random_state`.
- **Integration (needs Spark + MLflow):** run `build_features.py` then
  `train_anomaly_model.py`; assert a non-empty feature table and a registered
  model version in `Staging` with logged metrics. Documented; not runnable
  without a cluster.
