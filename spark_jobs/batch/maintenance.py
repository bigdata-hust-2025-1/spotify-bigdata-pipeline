"""Iceberg table maintenance — compaction, snapshot expiry, orphan cleanup.

Iceberg ``MERGE``/``createOrReplace`` writes (and streaming appends) accumulate
many small data files and a growing snapshot history. Left unmanaged this hurts
read planning and storage cost (finding J2). This job runs the three standard
Iceberg maintenance procedures over the Lakehouse tables:

* ``rewrite_data_files`` — compacts small files into right-sized ones.
* ``expire_snapshots``   — drops snapshots beyond a retained window.
* ``remove_orphan_files``— deletes files no live snapshot references.

The procedures are transactional and read-safe. This job is standalone now and
is scheduled by Airflow in PR-12.

The SQL builders are **pure** (no Spark, no I/O) so they are unit-tested without
a JVM, mirroring the ``common.spark`` helpers.
"""

import os
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.spark import (  # noqa: E402
    GOLD_NAMESPACE,
    LAKEHOUSE_CATALOG,
    SILVER_NAMESPACE,
    build_spark,
)

# Silver datasets (upserted via MERGE) and Gold datasets (aggregate tables).
SILVER_DATASETS = ["tracks", "albums", "artists", "owners", "playlists"]
GOLD_DATASETS = ["artists_stats", "albums_stats"]

# Number of most-recent snapshots to retain when expiring. A window (not zero)
# so time-travel/rollback stays possible; override with ``SNAPSHOT_RETAIN_LAST``.
SNAPSHOT_RETAIN_LAST = int(os.getenv("SNAPSHOT_RETAIN_LAST", "5"))


def default_tables():
    """Return the maintenance targets as ``namespace.table`` references.

    These are catalog-relative (no ``lakehouse.`` prefix) because the Iceberg
    procedures are *called on* the catalog and take the table as an argument.
    Override the whole set with a comma-separated ``MAINTENANCE_TABLES`` env var.
    """
    override = os.getenv("MAINTENANCE_TABLES")
    if override:
        return [t.strip() for t in override.split(",") if t.strip()]
    return (
        [f"{SILVER_NAMESPACE}.{d}" for d in SILVER_DATASETS]
        + [f"{GOLD_NAMESPACE}.{d}" for d in GOLD_DATASETS]
    )


def rewrite_data_files_sql(catalog: str, table_ref: str) -> str:
    """Build the ``rewrite_data_files`` (compaction) CALL for ``table_ref``."""
    if not table_ref:
        raise ValueError("table_ref must be a non-empty 'namespace.table'")
    return f"CALL {catalog}.system.rewrite_data_files(table => '{table_ref}')"


def expire_snapshots_sql(catalog: str, table_ref: str, retain_last: int) -> str:
    """Build the ``expire_snapshots`` CALL retaining ``retain_last`` snapshots."""
    if not table_ref:
        raise ValueError("table_ref must be a non-empty 'namespace.table'")
    if retain_last < 1:
        raise ValueError("retain_last must be >= 1 to keep at least one snapshot")
    return (
        f"CALL {catalog}.system.expire_snapshots("
        f"table => '{table_ref}', retain_last => {retain_last})"
    )


def remove_orphan_files_sql(catalog: str, table_ref: str) -> str:
    """Build the ``remove_orphan_files`` CALL for ``table_ref``."""
    if not table_ref:
        raise ValueError("table_ref must be a non-empty 'namespace.table'")
    return f"CALL {catalog}.system.remove_orphan_files(table => '{table_ref}')"


def maintain_table(spark, catalog: str, table_ref: str, retain_last: int):
    """Run the three maintenance procedures against one table."""
    print(f"\n=== MAINTENANCE: {catalog}.{table_ref} ===")
    for sql in (
        rewrite_data_files_sql(catalog, table_ref),
        expire_snapshots_sql(catalog, table_ref, retain_last),
        remove_orphan_files_sql(catalog, table_ref),
    ):
        try:
            spark.sql(sql)
            print(f"    [OK] {sql}")
        except Exception as e:
            print(f"    [ERROR] {sql} -> {e}")


if __name__ == "__main__":
    spark = build_spark("Spotify_Iceberg_Maintenance", iceberg=True)
    for table in default_tables():
        maintain_table(spark, LAKEHOUSE_CATALOG, table, SNAPSHOT_RETAIN_LAST)
    spark.stop()
