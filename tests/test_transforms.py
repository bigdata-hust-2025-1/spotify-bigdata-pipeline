"""Tests for PR-18 — native duration categorisation replacing the Python UDF.

The boundary semantics live in the pure ``categorize_duration`` (tested here
with no JVM). ``duration_category_expr`` is the native Spark expression that runs
in the hot path; an optional Spark-backed test proves the two agree on a value
matrix when a JVM is available — it is skipped in the JVM-free CI, exactly like
the repo's other Spark integration tests.

Runnable via pytest or ``python tests/test_transforms.py``.
"""

import os
import sys
from unittest import SkipTest as _Skip

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from spark_jobs.batch.bronze_to_silver_all import (  # noqa: E402
    DURATION_MEDIUM_MS,
    DURATION_SHORT_MS,
    categorize_duration,
    duration_category_expr,
)

# A value matrix that pins every boundary: None/0 (Unknown), just-below and
# exactly-on each threshold, and a clearly-long value.
_MATRIX = [None, 0, 1, 179_999, 180_000, 180_001, 299_999, 300_000, 300_001, 1_000_000]


def test_thresholds_are_milliseconds():
    # 3 minutes and 5 minutes, expressed in ms so no float division is needed.
    assert DURATION_SHORT_MS == 180_000
    assert DURATION_MEDIUM_MS == 300_000


def test_categorize_duration_boundaries():
    assert categorize_duration(None) == "Unknown"
    assert categorize_duration(0) == "Unknown"
    assert categorize_duration(1) == "Short"
    assert categorize_duration(179_999) == "Short"
    assert categorize_duration(180_000) == "Medium"   # exactly 3 min -> not < 180s
    assert categorize_duration(299_999) == "Medium"
    assert categorize_duration(300_000) == "Long"     # exactly 5 min -> not < 300s
    assert categorize_duration(1_000_000) == "Long"


def test_native_expr_matches_pure_function():
    """The native expression must equal the pure reference on every matrix value.

    Requires a Spark/JVM runtime; skipped where unavailable (bare CI), matching
    the repo's policy of importing pyspark but not running it in unit CI.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:  # pragma: no cover - pyspark absent
        raise _Skip("pyspark not installed")
    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("pr18-duration-equivalence")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:  # pragma: no cover - no Java/JVM available
        raise _Skip(f"no Spark runtime available: {exc}")

    try:
        df = spark.createDataFrame([(v,) for v in _MATRIX], "duration_ms: long")
        got = (
            df.withColumn("cat", duration_category_expr())
            .select("duration_ms", "cat")
            .collect()
        )
        for row in got:
            expected = categorize_duration(row["duration_ms"])
            assert row["cat"] == expected, (
                f"duration_ms={row['duration_ms']}: native={row['cat']} "
                f"!= pure={expected}"
            )
    finally:
        spark.stop()


def _run_all():
    tests = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    failures = []
    skipped = 0
    for t in tests:
        try:
            t()
            print(f"PASS - {t.__name__}")
        except _Skip as exc:
            print(f"SKIP - {t.__name__}: {exc}")
            skipped += 1
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL - {t.__name__}: {exc}")
            failures.append(t.__name__)
    print()
    if failures:
        print(f"{len(failures)} FAILED: {', '.join(failures)}")
        return 1
    print(f"ALL {len(tests) - skipped} TESTS PASSED ({skipped} skipped)")
    return 0


if __name__ == "__main__":
    raise SystemExit(_run_all())
