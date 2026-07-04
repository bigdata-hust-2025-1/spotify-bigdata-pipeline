"""Central Spark builder + Iceberg Lakehouse helpers.

Historically each batch job hand-rolled its own ``SparkSession.builder`` chain,
copy-pasting the S3A/MinIO wiring and — in the one orphaned Iceberg script —
pinning the **Spark 3.3** Iceberg runtime against a ``pyspark==3.5`` project
(finding C2, a guaranteed jar mismatch). This module makes the Spark session a
single source of truth:

* :func:`build_spark` — one builder with the MinIO S3A config and (optionally)
  the Iceberg 3.5 runtime + a Hadoop catalog whose warehouse lives on MinIO.
* :func:`silver_table` / :func:`build_merge_sql` — **pure** helpers (no Spark,
  no I/O) that name the Iceberg tables and generate the idempotent ``MERGE INTO``
  upsert. They are unit-tested without a JVM.

Design contract (mirrors :mod:`common.config`):

1. **Everything is env-overridable** — the Iceberg runtime coordinate, catalog
   name and warehouse location all read ``os.getenv`` with safe defaults.
2. **No secrets at import.** Credentials are resolved via
   :func:`common.config.require_env` *inside* :func:`build_spark` (fail-fast at
   session-build time), never at module import.
3. **Importing this module has no side effects** — the ``pyspark`` import is
   deferred into :func:`build_spark`, so the pure helpers (and the tests that
   cover them) import with no JVM present.
"""

import os

from common.config import MINIO_ENDPOINT, require_env

# ---------------------------------------------------------------------------
# Iceberg runtime + catalog configuration (all env-overridable)
# ---------------------------------------------------------------------------
# Must match the project's Spark/pyspark 3.5 line — the ``_3.5_`` classifier is
# the whole point of PR-07 (the orphan script pinned ``_3.3_`` against pyspark
# 3.5). Bump the trailing Iceberg version here in one place when upgrading.
ICEBERG_SPARK_RUNTIME = os.getenv(
    "ICEBERG_SPARK_RUNTIME",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
)

# Named catalog the Silver/Gold tables live under, e.g. ``lakehouse.silver.tracks``.
# A Hadoop catalog (metadata-on-object-store) keeps the setup dependency-free to
# start; swapping to a REST/Nessie catalog later is a pure config change.
LAKEHOUSE_CATALOG = os.getenv("LAKEHOUSE_CATALOG", "lakehouse")
LAKEHOUSE_WAREHOUSE = os.getenv(
    "LAKEHOUSE_WAREHOUSE", "s3a://spotify-lakehouse/warehouse"
)

# Iceberg namespace for the Silver medallion layer.
SILVER_NAMESPACE = os.getenv("SILVER_NAMESPACE", "silver")


def silver_table(dataset: str) -> str:
    """Return the fully-qualified Iceberg table identifier for a Silver dataset.

    e.g. ``silver_table("tracks") -> "lakehouse.silver.tracks"``. ``dataset``
    must be a non-empty simple name (it becomes part of a SQL identifier).
    """
    if not dataset:
        raise ValueError("dataset must be a non-empty table name")
    return f"{LAKEHOUSE_CATALOG}.{SILVER_NAMESPACE}.{dataset}"


def build_merge_sql(target_table: str, source_view: str, key_columns) -> str:
    """Build an idempotent Iceberg ``MERGE INTO`` upsert statement.

    Matching on the business ``key_columns`` makes re-running a load a no-op on
    the data (update-in-place / insert-new) instead of appending duplicates —
    this is what gives the acceptance-criterion "re-running the job is
    idempotent". ``UPDATE SET *`` / ``INSERT *`` keep the statement schema-driven
    so new Silver columns need no change here.

    Raises :class:`ValueError` if ``key_columns`` is empty — a MERGE with no join
    key would rewrite every row and silently break idempotency.
    """
    keys = list(key_columns)
    if not keys:
        raise ValueError(
            "key_columns must be non-empty; a MERGE without a business key "
            "cannot upsert idempotently"
        )
    on_clause = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    return (
        f"MERGE INTO {target_table} AS t\n"
        f"USING {source_view} AS s\n"
        f"ON {on_clause}\n"
        "WHEN MATCHED THEN UPDATE SET *\n"
        "WHEN NOT MATCHED THEN INSERT *"
    )


def build_spark(app_name: str, *, iceberg: bool = True):
    """Create a configured :class:`SparkSession`.

    Always wires the MinIO S3A filesystem (endpoint from
    :data:`common.config.MINIO_ENDPOINT`, credentials via
    :func:`common.config.require_env` — so a missing key fails fast rather than
    falling back to a well-known default). When ``iceberg`` is true it also
    registers the Iceberg 3.5 runtime, the SQL extensions, and the
    :data:`LAKEHOUSE_CATALOG` Hadoop catalog pointed at
    :data:`LAKEHOUSE_WAREHOUSE`.

    ``pyspark`` is imported lazily here so importing this module (and its pure
    helpers) needs no Spark/JVM.
    """
    from pyspark.sql import SparkSession

    access_key = require_env("MINIO_ACCESS_KEY")
    secret_key = require_env("MINIO_SECRET_KEY")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    if iceberg:
        builder = (
            builder.config("spark.jars.packages", ICEBERG_SPARK_RUNTIME)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                f"spark.sql.catalog.{LAKEHOUSE_CATALOG}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(f"spark.sql.catalog.{LAKEHOUSE_CATALOG}.type", "hadoop")
            .config(
                f"spark.sql.catalog.{LAKEHOUSE_CATALOG}.warehouse",
                LAKEHOUSE_WAREHOUSE,
            )
        )

    return builder.getOrCreate()
