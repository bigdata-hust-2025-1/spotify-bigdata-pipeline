"""Anomaly-detection training loop: user features -> IsolationForest -> MLflow (PR-16).

Previously this script trained on ``s3a://datalake/gold/user_behaviors.parquet``
— a path **no job wrote** — so it could never run end-to-end (findings D3, C6).
It now reads the per-user feature table produced by
``spark_jobs/batch/build_features.py`` (Parquet export at :data:`FEATURES_PATH`,
sourced from the star-schema fact ``gold_star.fact_playback``), trains an
Isolation Forest, and registers + stages the model in the MLflow Model Registry.

Structure (so the core is unit-testable without Spark or MLflow):

* :func:`train_isolation_forest` — **pure**: takes a pandas DataFrame, validates
  the :data:`FEATURE_COLUMNS` contract, fits a deterministic model, returns it.
  ``sklearn`` is imported lazily inside it; the module imports with no ML deps.
* :func:`load_features` — reads the Parquet feature snapshot into pandas.
* :func:`run_training` — the MLflow wrapper (deferred ``mlflow`` import): logs
  params/metrics, logs the model, registers it under :data:`MODEL_NAME`, and
  transitions the new version to ``Staging``.
"""

import os
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.logging import get_logger  # noqa: E402

LOG = get_logger("train_anomaly_model")

# The model input columns, in a fixed order — the same contract the feature
# producer publishes (``build_features.TRAINING_FEATURES``). Duplicated as a
# literal (not imported) so training has no dependency on pyspark.
FEATURE_COLUMNS = ["play_duration", "skip_count"]

# Registered model name in the MLflow Model Registry.
MODEL_NAME = os.getenv("ANOMALY_MODEL_NAME", "spotify_anomaly_isolation_forest")

# Where the feature snapshot lives — MUST match build_features.FEATURES_PATH.
FEATURES_PATH = os.getenv("FEATURES_PATH", "s3a://spotify-gold/features/user_features")

# Expected proportion of anomalies (Isolation Forest ``contamination``).
CONTAMINATION = float(os.getenv("ANOMALY_CONTAMINATION", "0.01"))

MLFLOW_TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI", "http://mlflow-server.bigdata:5000"
)
MLFLOW_STAGE = os.getenv("MLFLOW_MODEL_STAGE", "Staging")


def train_isolation_forest(
    features_df, contamination: float = CONTAMINATION, feature_columns=FEATURE_COLUMNS
):
    """Fit an Isolation Forest on the user-feature DataFrame (pure, no MLflow/Spark).

    Validates that every required feature column is present and that there is at
    least one row, so a broken feature table fails loudly here instead of
    producing a silently-degenerate model. ``random_state`` is fixed so a
    re-train on the same data is reproducible (and tests are deterministic).

    Raises :class:`ValueError` on a missing column or empty input.
    """
    missing = [c for c in feature_columns if c not in features_df.columns]
    if missing:
        raise ValueError(
            f"feature table is missing required column(s) {missing}; "
            f"expected {feature_columns} (produced by build_features.py)"
        )
    if len(features_df) == 0:
        raise ValueError("feature table has no rows to train on")

    from sklearn.ensemble import IsolationForest  # deferred

    model = IsolationForest(contamination=contamination, random_state=42)
    model.fit(features_df[feature_columns])
    return model


def load_features(path: str = FEATURES_PATH):
    """Read the Parquet feature snapshot written by ``build_features.py``."""
    import pandas as pd  # deferred

    LOG.info("load_features", extra={"path": path})
    return pd.read_parquet(path)


def run_training(
    features_path: str = FEATURES_PATH,
    contamination: float = CONTAMINATION,
    model_name: str = MODEL_NAME,
    stage: str = MLFLOW_STAGE,
):
    """Full loop: load features, train, log + register the model, stage it.

    Logs ``contamination`` / row + feature counts as params and the detected
    anomaly count/rate as metrics, then registers the model under ``model_name``
    and transitions the new version to ``stage`` in the MLflow registry.
    """
    import mlflow  # deferred
    import mlflow.sklearn  # deferred

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    with mlflow.start_run() as run:
        features_df = load_features(features_path)
        model = train_isolation_forest(features_df, contamination)

        predictions = model.predict(features_df[FEATURE_COLUMNS])
        n_rows = int(len(features_df))
        anomaly_count = int((predictions == -1).sum())

        mlflow.log_params(
            {
                "contamination": contamination,
                "n_features": len(FEATURE_COLUMNS),
                "feature_columns": ",".join(FEATURE_COLUMNS),
                "n_training_rows": n_rows,
            }
        )
        mlflow.log_metrics(
            {
                "anomaly_count": anomaly_count,
                "anomaly_rate": (anomaly_count / n_rows) if n_rows else 0.0,
            }
        )

        mlflow.sklearn.log_model(
            model, artifact_path="model", registered_model_name=model_name
        )
        _transition_latest_to_stage(mlflow, model_name, stage)
        LOG.info(
            "training_done",
            extra={
                "run_id": run.info.run_id,
                "model": model_name,
                "stage": stage,
                "anomaly_count": anomaly_count,
                "n_rows": n_rows,
            },
        )


def _transition_latest_to_stage(mlflow, model_name: str, stage: str) -> None:
    """Move the newest registered version of ``model_name`` to ``stage``."""
    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions(f"name='{model_name}'")
    if not versions:
        return
    latest = max(versions, key=lambda v: int(v.version))
    client.transition_model_version_stage(
        name=model_name, version=latest.version, stage=stage
    )


if __name__ == "__main__":
    run_training()
