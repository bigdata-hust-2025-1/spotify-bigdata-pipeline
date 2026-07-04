"""Tests for the shared streaming schema and its Cassandra contract.

Uses only ``pyspark.sql.types`` (pure Python, no JVM), so it runs without a
Spark cluster. Runnable via pytest or ``python tests/test_event_schema.py``.
"""

import os
import re
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from spark_jobs.stream.event_schema import (  # noqa: E402
    DERIVED_USER_PLAYS_COLUMNS,
    PLAYBACK_EVENT_SCHEMA,
    USER_PLAYS_COLUMNS,
)

_CQL_PATH = os.path.join(_REPO_ROOT, "cassandra", "user_plays.cql")

_EXPECTED_EVENT_FIELDS = [
    "track_id", "track_name", "artist_id", "album_id", "track_popularity",
    "event_id", "user_id", "timestamp", "event_time_str", "location",
    "device", "listen_duration_ms", "status",
]


def _cql_table_columns(path):
    """Extract the non-key column names from the CREATE TABLE body."""
    text = open(path, encoding="utf-8").read()
    body = text[text.index("user_plays"):]
    body = body[body.index("(") + 1:]
    cols = []
    for raw in body.splitlines():
        line = raw.strip()
        if line.upper().startswith("PRIMARY KEY"):
            break
        m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s+[A-Za-z]", line)
        if m:
            cols.append(m.group(1))
    return cols


def test_event_schema_field_names_match_producer():
    names = [f.name for f in PLAYBACK_EVENT_SCHEMA.fields]
    assert names == _EXPECTED_EVENT_FIELDS


def test_user_plays_columns_are_schema_fields_or_derived():
    schema_fields = {f.name for f in PLAYBACK_EVENT_SCHEMA.fields}
    allowed = schema_fields | DERIVED_USER_PLAYS_COLUMNS
    unknown = [c for c in USER_PLAYS_COLUMNS if c not in allowed]
    assert not unknown, f"columns not backed by schema/derived: {unknown}"


def test_user_plays_columns_match_cassandra_ddl():
    cql_cols = _cql_table_columns(_CQL_PATH)
    assert set(USER_PLAYS_COLUMNS) == set(cql_cols), (
        f"Spark write columns {sorted(USER_PLAYS_COLUMNS)} != "
        f"Cassandra table columns {sorted(cql_cols)}"
    )


def test_no_duplicate_user_plays_columns():
    assert len(USER_PLAYS_COLUMNS) == len(set(USER_PLAYS_COLUMNS))


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
