"""Tests for the data-quality gates in ``spark_jobs.quality.checks`` (PR-15).

Pure stdlib — no Spark/JVM. Covers the evaluator decision logic on seeded
null-key / duplicate / broken-FK / out-of-range / stale fixtures, that a
BLOCK-severity breach raises (fails the pipeline) while WARN does not, that clean
data passes, and that results log as structured ``dq_check`` metrics.

Runnable via pytest or ``python tests/test_quality.py``.
"""

import io
import json
import logging
import os
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.logging import JsonFormatter  # noqa: E402
from spark_jobs.quality import checks as q  # noqa: E402


def test_non_null_keys_pass_and_fail():
    assert q.non_null_keys("k", 0).passed is True
    bad = q.non_null_keys("k", 3)
    assert bad.passed is False
    assert bad.observed == 3 and bad.blocking is True  # default severity=block


def test_unique_keys_detects_duplicates():
    assert q.unique_keys("k", total=10, distinct=10).passed is True
    dup = q.unique_keys("k", total=10, distinct=8)
    assert dup.passed is False and dup.observed == 2


def test_fk_resolved_pass_and_fail():
    assert q.fk_resolved("fk", 0).passed is True
    assert q.fk_resolved("fk", 5).passed is False


def test_value_in_range():
    assert q.value_in_range("pop", 0).passed is True
    assert q.value_in_range("pop", 4).passed is False


def test_freshness_within_and_beyond_and_missing():
    assert q.freshness("f", lag_days=1, max_lag_days=2).passed is True
    assert q.freshness("f", lag_days=5, max_lag_days=2).passed is False
    assert q.freshness("f", lag_days=None, max_lag_days=2).passed is False  # no data


def test_warn_severity_does_not_block():
    warn = q.non_null_keys("k", 3, severity=q.WARN)
    assert warn.passed is False
    assert warn.blocking is False  # a warning is not a blocker


def test_report_raises_on_blocking_failure():
    report = q.DataQualityReport("silver.tracks")
    report.add(q.non_null_keys("silver.tracks.non_null_key", 0))       # clean
    report.add(q.unique_keys("silver.tracks.unique_key", 10, 9))       # 1 duplicate
    assert len(report.failed) == 1
    try:
        report.raise_if_failed()
    except q.DataQualityError as exc:
        assert "unique_key" in str(exc)
        return
    raise AssertionError("a blocking failure must raise DataQualityError")


def test_report_clean_data_passes_silently():
    report = q.DataQualityReport("silver.artists")
    report.add(q.non_null_keys("silver.artists.non_null_key", 0))
    report.add(q.unique_keys("silver.artists.unique_key", 5, 5))
    assert report.failed == []
    report.raise_if_failed()  # must not raise


def test_report_warn_only_does_not_raise():
    report = q.DataQualityReport("silver.albums")
    report.add(q.non_null_keys("silver.albums.non_null_key", 2, severity=q.WARN))
    assert len(report.failed) == 1 and report.blocking == []
    report.raise_if_failed()  # warn-only → no raise


def test_report_logs_each_check_as_json_metric():
    logger = logging.getLogger("t.dq")
    for h in list(logger.handlers):
        logger.removeHandler(h)
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    report = q.DataQualityReport("silver.tracks")
    report.add(q.non_null_keys("silver.tracks.non_null_key", 0))
    report.add(q.unique_keys("silver.tracks.unique_key", 3, 3))
    report.log(logger)

    recs = [json.loads(ln) for ln in buf.getvalue().splitlines() if ln.strip()]
    assert len(recs) == 2
    assert all(r["event"] == "dq_check" and r["layer"] == "silver.tracks" for r in recs)
    assert {r["check"] for r in recs} == {
        "silver.tracks.non_null_key", "silver.tracks.unique_key"
    }


def _run_all():
    tests = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    failures = []
    for t in tests:
        try:
            t()
            print(f"PASS - {t.__name__}")
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL - {t.__name__}: {exc}")
            failures.append(t.__name__)
    print()
    if failures:
        print(f"{len(failures)} FAILED: {', '.join(failures)}")
        return 1
    print(f"ALL {len(tests)} TESTS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(_run_all())
