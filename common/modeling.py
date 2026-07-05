"""Dimensional-model helpers — star-schema naming + SCD2 SQL generation.

PR-09 introduces a conformed star schema (``dim_artist``/``dim_album``/
``dim_track`` + ``fact_playback``) in a **new** ``gold_star`` namespace, additive
to the legacy ``*_stats`` Gold tables (which stay untouched until a cutover).

Everything here is **pure** (no Spark, no I/O): table naming, the deterministic
surrogate-key / change-detection expressions, and the three SCD2 statements
(create-staging is done in the driver; close + insert are generated here). This
keeps the modelling logic unit-testable without a JVM, mirroring
``common.spark``'s ``build_merge_sql``.

SCD2 (slowly-changing dimension, type 2) keeps history: when a tracked attribute
of a business key changes, the current row is *closed* (``is_current=false``,
``valid_to`` stamped) and a *new* version row is inserted. The load is a
deterministic, idempotent three-step (build staging → close changed → insert
new/changed), so re-running the same input is a no-op.
"""

import os

from common.spark import LAKEHOUSE_CATALOG

# Namespace for the dimensional (star) model. Separate from the legacy ``gold``
# aggregates so the star tables are purely additive; override with
# ``GOLD_STAR_NAMESPACE``.
GOLD_STAR_NAMESPACE = os.getenv("GOLD_STAR_NAMESPACE", "gold_star")

# The bookkeeping columns every SCD2 dimension carries.
SCD2_COLUMNS = ["surrogate_key", "attr_hash", "valid_from", "valid_to", "is_current"]

# Sentinel used so a real NULL and the empty string hash differently, and so two
# rows differing only by a NULL are detected as changed.
_NULL_SENTINEL = "__NULL__"


def star_table(name: str) -> str:
    """Return the fully-qualified identifier for a star-schema table.

    e.g. ``star_table("dim_artist") -> "lakehouse.gold_star.dim_artist"``.
    """
    if not name:
        raise ValueError("name must be a non-empty table name")
    return f"{LAKEHOUSE_CATALOG}.{GOLD_STAR_NAMESPACE}.{name}"


def _coalesced(col: str) -> str:
    return f"coalesce(cast({col} as string), '{_NULL_SENTINEL}')"


def attribute_hash_expr(attr_columns) -> str:
    """SQL expression hashing the tracked attributes for SCD2 change detection.

    Null-safe (a NULL is distinct from ''), order-sensitive, and stable across
    runs, so an unchanged row hashes identically (→ idempotent load) and any
    tracked change flips the hash (→ a new version).
    """
    cols = list(attr_columns)
    if not cols:
        raise ValueError("attr_columns must be non-empty")
    joined = ", ".join(_coalesced(c) for c in cols)
    return f"sha2(concat_ws('||', {joined}), 256)"


def surrogate_key_expr(
    business_key_columns, valid_from_column: str = "valid_from"
) -> str:
    """SQL expression for a deterministic per-version surrogate key.

    Hashes the business key together with ``valid_from`` so every *version* of a
    business key gets a unique, reproducible key (reproducible = a retried load
    yields the same key, which keeps the insert idempotent).
    """
    keys = list(business_key_columns)
    if not keys:
        raise ValueError("business_key_columns must be non-empty")
    parts = ", ".join(_coalesced(c) for c in keys)
    return f"sha2(concat_ws('||', {parts}, cast({valid_from_column} as string)), 256)"


def _on_clause(business_key_columns, left: str, right: str) -> str:
    keys = list(business_key_columns)
    if not keys:
        raise ValueError("business_key_columns must be non-empty")
    return " AND ".join(f"{left}.{k} = {right}.{k}" for k in keys)


def scd2_close_sql(table: str, staging: str, business_key_columns) -> str:
    """MERGE that closes the current row of any business key whose attributes changed.

    Matches the open (``is_current=true``) row to staging on the business key and,
    only when the attribute hash differs, stamps ``valid_to`` and clears
    ``is_current``. Unchanged keys are untouched → the statement is idempotent.
    """
    on = _on_clause(business_key_columns, "t", "s")
    return (
        f"MERGE INTO {table} AS t\n"
        f"USING {staging} AS s\n"
        f"ON {on} AND t.is_current = true\n"
        "WHEN MATCHED AND t.attr_hash <> s.attr_hash THEN\n"
        "  UPDATE SET t.is_current = false, t.valid_to = s.valid_from"
    )


def scd2_insert_sql(table: str, staging: str, business_key_columns) -> str:
    """INSERT new version rows for business keys with no currently-open row.

    Run *after* :func:`scd2_close_sql`: a changed key's old row is already closed,
    so it (and any brand-new key) has no ``is_current=true`` row and is inserted
    as a fresh version; unchanged keys still have an open row and are skipped
    (``LEFT ANTI JOIN``) → idempotent.
    """
    on = _on_clause(business_key_columns, "s", "c")
    key_select = ", ".join(business_key_columns)
    open_rows = f"(SELECT {key_select} FROM {table} WHERE is_current = true)"
    return (
        f"INSERT INTO {table}\n"
        f"SELECT s.* FROM {staging} AS s\n"
        f"LEFT ANTI JOIN {open_rows} AS c\n"
        f"ON {on}"
    )


def unresolved_fk_count_sql(fact_view: str, surrogate_key_columns) -> str:
    """Count fact rows where any dimension surrogate key failed to resolve.

    Backs the referential-integrity acceptance criterion: a healthy load returns
    0 (every fact foreign key resolves to a dimension row).
    """
    cols = list(surrogate_key_columns)
    if not cols:
        raise ValueError("surrogate_key_columns must be non-empty")
    predicate = " OR ".join(f"{c} IS NULL" for c in cols)
    return f"SELECT count(*) AS unresolved FROM {fact_view} WHERE {predicate}"
