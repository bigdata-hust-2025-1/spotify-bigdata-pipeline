"""Tests for the MLOps training core in ``mlops.train_anomaly_model`` (PR-16).

Pure pandas + scikit-learn — no Spark/JVM and no MLflow (the ``mlflow`` import is
deferred into ``run_training``, which is not exercised here). Covers the feature
contract, that a planted outlier is flagged, and that a broken/empty feature
table fails loudly instead of training a degenerate model.

Runnable via pytest or ``python tests/test_mlops.py``.
"""

import os
import sys
from unittest import SkipTest as _Skip

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

try:
    import pandas as pd
except ImportError:  # pragma: no cover - pandas absent in a bare env
    raise _Skip("pandas not installed")

from mlops import train_anomaly_model as t  # noqa: E402


def _normal_users(n=40):
    """A block of unremarkable users clustered tightly together."""
    return pd.DataFrame(
        {
            "user_id": [f"u{i}" for i in range(n)],
            "play_duration": [200000 + (i % 5) * 1000 for i in range(n)],
            "skip_count": [1 + (i % 3) for i in range(n)],
        }
    )


def test_feature_columns_contract():
    # The trainer's inputs must match what build_features publishes.
    assert t.FEATURE_COLUMNS == ["play_duration", "skip_count"]


def test_train_returns_fitted_model():
    model = t.train_isolation_forest(_normal_users())
    # A fitted IsolationForest can score the training frame.
    preds = model.predict(_normal_users()[t.FEATURE_COLUMNS])
    assert set(preds).issubset({-1, 1})


def test_planted_outlier_is_flagged():
    df = _normal_users()
    # One user skips wildly more and listens far longer than everyone else.
    outlier = pd.DataFrame(
        {"user_id": ["anomaly"], "play_duration": [5_000_000], "skip_count": [999]}
    )
    df = pd.concat([df, outlier], ignore_index=True)
    model = t.train_isolation_forest(df, contamination=0.05)
    preds = model.predict(df[t.FEATURE_COLUMNS])
    flagged = df.loc[preds == -1, "user_id"].tolist()
    assert "anomaly" in flagged


def test_missing_feature_column_raises():
    bad = pd.DataFrame({"user_id": ["u1"], "play_duration": [1000]})  # no skip_count
    try:
        t.train_isolation_forest(bad)
    except ValueError as exc:
        assert "skip_count" in str(exc)
        return
    raise AssertionError("a missing feature column must raise ValueError")


def test_empty_feature_table_raises():
    empty = pd.DataFrame({c: [] for c in ["user_id", *t.FEATURE_COLUMNS]})
    try:
        t.train_isolation_forest(empty)
    except ValueError as exc:
        assert "no rows" in str(exc)
        return
    raise AssertionError("an empty feature table must raise ValueError")


def test_training_is_reproducible():
    # Fixed random_state -> identical predictions across retrains.
    df = _normal_users()
    p1 = t.train_isolation_forest(df).predict(df[t.FEATURE_COLUMNS])
    p2 = t.train_isolation_forest(df).predict(df[t.FEATURE_COLUMNS])
    assert list(p1) == list(p2)


def _run_all():
    tests = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    failures = []
    for fn in tests:
        try:
            fn()
            print(f"PASS - {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL - {fn.__name__}: {exc}")
            failures.append(fn.__name__)
    print()
    if failures:
        print(f"{len(failures)} FAILED: {', '.join(failures)}")
        return 1
    print(f"ALL {len(tests)} TESTS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(_run_all())
