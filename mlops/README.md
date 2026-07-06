# mlops/

Anomaly-detection training loop. Reads the per-user feature table produced by
`spark_jobs/batch/build_features.py`, trains an Isolation Forest, and registers
the model in MLflow.

| File | Role |
| :--- | :--- |
| `train_anomaly_model.py` | Pure `train_isolation_forest()` core (pandas + sklearn, deterministic, unit-tested) + `run_training()` MLflow wrapper (log params/metrics, `log_model`, register, transition to `Staging`). |

Heavy deps (`mlflow`, `sklearn`) are deferred so the module imports without them.
Feature/trainer contract and run commands are documented in `docs/MLOPS.md`;
tests live in `tests/test_mlops.py`.
