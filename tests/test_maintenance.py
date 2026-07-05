"""Tests for the pure Iceberg-maintenance SQL builders in ``maintenance``.

These cover the compaction / snapshot-expiry / orphan-cleanup CALL generation
that backs PR-08's table maintenance. They import ``spark_jobs.batch.maintenance``
(whose ``pyspark`` use is confined to ``build_spark``), so they run with no
Spark/JVM present. Runnable via pytest or ``python tests/test_maintenance.py``.
"""

import os
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from spark_jobs.batch import maintenance as m  # noqa: E402


def test_rewrite_data_files_sql():
    sql = m.rewrite_data_files_sql("lakehouse", "silver.tracks")
    assert sql == (
        "CALL lakehouse.system.rewrite_data_files(table => 'silver.tracks')"
    )


def test_expire_snapshots_sql_uses_retain_last():
    sql = m.expire_snapshots_sql("lakehouse", "gold.artists_stats", 5)
    assert "CALL lakehouse.system.expire_snapshots(" in sql
    assert "table => 'gold.artists_stats'" in sql
    assert "retain_last => 5" in sql


def test_expire_snapshots_rejects_zero_retain():
    try:
        m.expire_snapshots_sql("lakehouse", "silver.tracks", 0)
    except ValueError:
        return
    raise AssertionError("retain_last < 1 should raise ValueError")


def test_remove_orphan_files_sql():
    sql = m.remove_orphan_files_sql("lakehouse", "silver.albums")
    assert sql == (
        "CALL lakehouse.system.remove_orphan_files(table => 'silver.albums')"
    )


def test_builders_reject_empty_table():
    for fn in (m.rewrite_data_files_sql, m.remove_orphan_files_sql):
        try:
            fn("lakehouse", "")
        except ValueError:
            continue
        raise AssertionError(f"{fn.__name__}('') should raise ValueError")


def test_default_tables_cover_silver_and_gold():
    prev = os.environ.pop("MAINTENANCE_TABLES", None)
    try:
        tables = m.default_tables()
        # Every Silver and Gold dataset must be a maintenance target.
        for d in m.SILVER_DATASETS:
            assert f"silver.{d}" in tables
        for d in m.GOLD_DATASETS:
            assert f"gold.{d}" in tables
    finally:
        if prev is not None:
            os.environ["MAINTENANCE_TABLES"] = prev


def test_default_tables_honours_override():
    prev = os.environ.get("MAINTENANCE_TABLES")
    os.environ["MAINTENANCE_TABLES"] = "silver.tracks, gold.albums_stats"
    try:
        assert m.default_tables() == ["silver.tracks", "gold.albums_stats"]
    finally:
        if prev is None:
            os.environ.pop("MAINTENANCE_TABLES", None)
        else:
            os.environ["MAINTENANCE_TABLES"] = prev


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
