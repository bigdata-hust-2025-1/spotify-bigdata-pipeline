"""Tests for the pure Iceberg-Lakehouse helpers in ``common.spark``.

These cover the table-naming and MERGE-SQL generation that back PR-07's
idempotent Silver upsert. They import ``common.spark`` (whose ``pyspark`` import
is deferred into ``build_spark``), so they run with no Spark/JVM present.
Runnable via pytest or ``python tests/test_spark.py``.
"""

import importlib
import os
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common import spark as spark_mod  # noqa: E402
from common.spark import build_merge_sql, gold_table, silver_table  # noqa: E402
from spark_jobs.batch.bronze_to_silver_all import BUSINESS_KEYS  # noqa: E402


def test_silver_table_default_identifier():
    assert silver_table("tracks") == "lakehouse.silver.tracks"


def test_silver_table_rejects_empty():
    try:
        silver_table("")
    except ValueError:
        return
    raise AssertionError("silver_table('') should raise ValueError")


def test_gold_table_default_identifier():
    assert gold_table("artists_stats") == "lakehouse.gold.artists_stats"


def test_gold_table_rejects_empty():
    try:
        gold_table("")
    except ValueError:
        return
    raise AssertionError("gold_table('') should raise ValueError")


def test_silver_and_gold_share_catalog_but_differ_by_namespace():
    s = silver_table("x").split(".")
    g = gold_table("x").split(".")
    assert s[0] == g[0]          # same catalog
    assert s[1] != g[1]          # different namespace (silver vs gold)


def test_silver_table_honours_env_overrides():
    prev = {k: os.environ.get(k) for k in ("LAKEHOUSE_CATALOG", "SILVER_NAMESPACE")}
    os.environ["LAKEHOUSE_CATALOG"] = "warehouse_cat"
    os.environ["SILVER_NAMESPACE"] = "clean"
    try:
        reloaded = importlib.reload(spark_mod)
        assert reloaded.silver_table("albums") == "warehouse_cat.clean.albums"
    finally:
        for k, v in prev.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        importlib.reload(spark_mod)  # restore module-level defaults


def test_iceberg_runtime_targets_spark_3_5():
    # The whole point of PR-07: the runtime classifier must be the 3.5 line,
    # not the orphan script's 3.3.
    assert "iceberg-spark-runtime-3.5_" in spark_mod.ICEBERG_SPARK_RUNTIME
    assert "3.3_" not in spark_mod.ICEBERG_SPARK_RUNTIME


def test_build_merge_sql_single_key():
    sql = build_merge_sql("lakehouse.silver.tracks", "_src_tracks", ["track_id"])
    assert "MERGE INTO lakehouse.silver.tracks AS t" in sql
    assert "USING _src_tracks AS s" in sql
    assert "ON t.track_id = s.track_id" in sql
    assert "WHEN MATCHED THEN UPDATE SET *" in sql
    assert "WHEN NOT MATCHED THEN INSERT *" in sql


def test_build_merge_sql_composite_key_uses_and():
    sql = build_merge_sql("cat.ns.t", "_src", ["a", "b"])
    assert "ON t.a = s.a AND t.b = s.b" in sql


def test_build_merge_sql_rejects_empty_keys():
    try:
        build_merge_sql("cat.ns.t", "_src", [])
    except ValueError:
        return
    raise AssertionError("empty key_columns should raise ValueError")


def test_every_dataset_has_a_business_key():
    # Every Silver dataset the job processes must have a non-empty MERGE key,
    # otherwise the Iceberg upsert would rewrite every row.
    for dataset, keys in BUSINESS_KEYS.items():
        assert keys, f"{dataset} has no business key"
        # And that key must produce a valid MERGE (exercises the guard).
        build_merge_sql(silver_table(dataset), f"_src_{dataset}", keys)


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
