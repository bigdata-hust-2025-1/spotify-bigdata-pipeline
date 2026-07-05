"""Tests for the pure star-schema / SCD2 helpers in ``common.modeling``.

These cover the table naming, surrogate-key / attribute-hash expressions, the
three SCD2 statements, and the referential-integrity check that back PR-09's
dimensional model. They import ``common.modeling`` (pure — no Spark/JVM), so they
run without a cluster. Runnable via pytest or ``python tests/test_modeling.py``.
"""

import os
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.modeling import (  # noqa: E402
    attribute_hash_expr,
    scd2_close_sql,
    scd2_insert_sql,
    star_table,
    surrogate_key_expr,
    unresolved_fk_count_sql,
)


def test_star_table_default_identifier():
    assert star_table("dim_artist") == "lakehouse.gold_star.dim_artist"


def test_star_table_rejects_empty():
    try:
        star_table("")
    except ValueError:
        return
    raise AssertionError("star_table('') should raise ValueError")


def test_attribute_hash_expr_is_null_safe_and_covers_all_columns():
    expr = attribute_hash_expr(["name", "popularity"])
    assert expr.startswith("sha2(concat_ws('||', ")
    assert expr.endswith(", 256)")
    # Null-safe: each column coalesced against the sentinel.
    assert "coalesce(cast(name as string), '__NULL__')" in expr
    assert "coalesce(cast(popularity as string), '__NULL__')" in expr


def test_attribute_hash_expr_rejects_empty():
    try:
        attribute_hash_expr([])
    except ValueError:
        return
    raise AssertionError("empty attr_columns should raise ValueError")


def test_surrogate_key_expr_binds_valid_from():
    expr = surrogate_key_expr(["artist_id"])
    assert "cast(valid_from as string)" in expr
    assert "coalesce(cast(artist_id as string), '__NULL__')" in expr


def test_surrogate_key_expr_rejects_empty():
    try:
        surrogate_key_expr([])
    except ValueError:
        return
    raise AssertionError("empty business_key_columns should raise ValueError")


def test_scd2_close_matches_open_row_and_closes_on_change():
    sql = scd2_close_sql("cat.ns.dim_artist", "_stg", ["artist_id"])
    assert "MERGE INTO cat.ns.dim_artist AS t" in sql
    assert "ON t.artist_id = s.artist_id AND t.is_current = true" in sql
    assert "WHEN MATCHED AND t.attr_hash <> s.attr_hash THEN" in sql
    assert "UPDATE SET t.is_current = false, t.valid_to = s.valid_from" in sql


def test_scd2_insert_uses_left_anti_join_on_open_rows():
    sql = scd2_insert_sql("cat.ns.dim_artist", "_stg", ["artist_id"])
    assert "INSERT INTO cat.ns.dim_artist" in sql
    assert "SELECT s.* FROM _stg AS s" in sql
    assert "LEFT ANTI JOIN" in sql
    assert "WHERE is_current = true" in sql
    assert "ON s.artist_id = c.artist_id" in sql


def test_scd2_composite_business_key_uses_and():
    close = scd2_close_sql("t", "s", ["a", "b"])
    assert "ON t.a = s.a AND t.b = s.b AND t.is_current = true" in close


def test_unresolved_fk_count_sql_ors_all_keys():
    sql = unresolved_fk_count_sql("_fact", ["track_sk", "artist_sk"])
    assert sql == (
        "SELECT count(*) AS unresolved FROM _fact "
        "WHERE track_sk IS NULL OR artist_sk IS NULL"
    )


def test_unresolved_fk_count_sql_rejects_empty():
    try:
        unresolved_fk_count_sql("_fact", [])
    except ValueError:
        return
    raise AssertionError("empty surrogate_key_columns should raise ValueError")


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
