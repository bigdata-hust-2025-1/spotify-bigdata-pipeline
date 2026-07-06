"""Data-quality gates between medallion layers (PR-15).

Reusable checks — non-null keys, key uniqueness, Silver→Gold FK resolution,
value range, and freshness — so bad data fails the pipeline instead of flowing
silently to Gold / serving (finding H2).

The decision logic (``CheckResult`` + the evaluator functions + ``DataQualityReport``)
is pure Python and unit-testable without a JVM. The DataFrame-executing helpers
(``key_checks``/``range_check``/``fk_check``/``freshness_check``) defer their
``pyspark`` import, so this module imports anywhere — matching ``common.spark`` /
``common.modeling``.

Each check yields a ``CheckResult``; a ``DataQualityReport`` aggregates them, logs
each as a structured metric, and ``raise_if_failed()`` raises ``DataQualityError``
on any BLOCK-severity breach. Severity is configurable (``DATA_QUALITY_SEVERITY``
= ``block`` (default) or ``warn``) so gates can be run in warn-only mode.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from typing import Any, Optional

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

WARN = "warn"
BLOCK = "block"

# Default gate severity. DATA_QUALITY_SEVERITY=warn downgrades every gate to a
# non-blocking warning (still logged) — useful when first rolling gates out.
DEFAULT_SEVERITY = os.getenv("DATA_QUALITY_SEVERITY", BLOCK).lower()


class DataQualityError(RuntimeError):
    """Raised when one or more BLOCK-severity checks fail."""


@dataclass
class CheckResult:
    name: str
    passed: bool
    severity: str = DEFAULT_SEVERITY
    observed: Any = None
    threshold: Any = None
    details: str = ""

    @property
    def blocking(self) -> bool:
        """A failed check only blocks the pipeline at BLOCK severity."""
        return (not self.passed) and self.severity == BLOCK

    def as_dict(self) -> dict:
        return {
            "check": self.name,
            "passed": self.passed,
            "severity": self.severity,
            "observed": self.observed,
            "threshold": self.threshold,
            "details": self.details,
        }


# --- Pure evaluators (no Spark): given a computed metric, decide pass/fail. -----

def non_null_keys(name: str, null_count: int, severity: str = DEFAULT_SEVERITY) -> CheckResult:
    return CheckResult(
        name, passed=(null_count == 0), severity=severity,
        observed=null_count, threshold=0,
        details=f"{null_count} row(s) with a null key column",
    )


def unique_keys(
    name: str, total: int, distinct: int, severity: str = DEFAULT_SEVERITY
) -> CheckResult:
    duplicates = total - distinct
    return CheckResult(
        name, passed=(duplicates == 0), severity=severity,
        observed=duplicates, threshold=0,
        details=f"{duplicates} duplicate key(s) ({total} rows, {distinct} distinct)",
    )


def fk_resolved(
    name: str, unresolved_count: int, severity: str = DEFAULT_SEVERITY
) -> CheckResult:
    return CheckResult(
        name, passed=(unresolved_count == 0), severity=severity,
        observed=unresolved_count, threshold=0,
        details=f"{unresolved_count} fact row(s) with an unresolved surrogate key",
    )


def value_in_range(
    name: str, out_of_range_count: int, severity: str = DEFAULT_SEVERITY
) -> CheckResult:
    return CheckResult(
        name, passed=(out_of_range_count == 0), severity=severity,
        observed=out_of_range_count, threshold=0,
        details=f"{out_of_range_count} row(s) outside the allowed range",
    )


def freshness(
    name: str, lag_days: Optional[int], max_lag_days: int,
    severity: str = DEFAULT_SEVERITY,
) -> CheckResult:
    ok = lag_days is not None and lag_days <= max_lag_days
    return CheckResult(
        name, passed=ok, severity=severity,
        observed=lag_days, threshold=max_lag_days,
        details=(
            "no data found" if lag_days is None
            else f"data is {lag_days} day(s) old (max {max_lag_days})"
        ),
    )


# --- Aggregation + reporting ---------------------------------------------------

@dataclass
class DataQualityReport:
    layer: str
    results: list = field(default_factory=list)

    def add(self, result: CheckResult) -> CheckResult:
        self.results.append(result)
        return result

    @property
    def failed(self) -> list:
        return [r for r in self.results if not r.passed]

    @property
    def blocking(self) -> list:
        return [r for r in self.results if r.blocking]

    def log(self, logger) -> None:
        """Emit every check result as a structured metric record."""
        for r in self.results:
            logger.info(
                "dq_check",
                extra={"event": "dq_check", "layer": self.layer, **r.as_dict()},
            )

    def raise_if_failed(self) -> None:
        blockers = self.blocking
        if blockers:
            names = ", ".join(r.name for r in blockers)
            raise DataQualityError(
                f"{len(blockers)} data-quality gate(s) failed in {self.layer}: {names}"
            )


# --- Spark-executing helpers (pyspark imported lazily) -------------------------

def key_checks(
    df, key_columns, layer: str, severity: str = DEFAULT_SEVERITY
) -> DataQualityReport:
    """Non-null + uniqueness gates on a dataset's business key."""
    from pyspark.sql import functions as F  # deferred

    report = DataQualityReport(layer)
    total = df.count()

    null_cond = None
    for c in key_columns:
        cond = F.col(c).isNull()
        null_cond = cond if null_cond is None else (null_cond | cond)
    null_count = df.filter(null_cond).count() if null_cond is not None else 0
    report.add(non_null_keys(f"{layer}.non_null_key", null_count, severity))

    distinct = df.select(*key_columns).distinct().count() if key_columns else total
    report.add(unique_keys(f"{layer}.unique_key", total, distinct, severity))
    return report


def range_check(
    df, column: str, low, high, layer: str, severity: str = DEFAULT_SEVERITY
) -> CheckResult:
    """Gate that ``column`` values fall within ``[low, high]`` (nulls ignored)."""
    from pyspark.sql import functions as F  # deferred

    oor = df.filter(
        F.col(column).isNotNull() & ((F.col(column) < low) | (F.col(column) > high))
    ).count()
    return value_in_range(f"{layer}.{column}_range", oor, severity)


def fk_check(
    spark, fact_view: str, surrogate_key_columns, layer: str,
    severity: str = DEFAULT_SEVERITY,
) -> CheckResult:
    """Gate that every fact row resolved its dimension surrogate keys (Silver→Gold)."""
    from common.modeling import unresolved_fk_count_sql  # deferred (no pyspark here)

    unresolved = spark.sql(
        unresolved_fk_count_sql(fact_view, surrogate_key_columns)
    ).collect()[0][0]
    return fk_resolved(f"{layer}.fk_resolution", unresolved, severity)


def freshness_check(
    df, date_column: str, as_of_date: str, max_lag_days: int, layer: str,
    severity: str = DEFAULT_SEVERITY,
) -> CheckResult:
    """Gate that the newest ``date_column`` is within ``max_lag_days`` of ``as_of_date``."""
    from datetime import date  # stdlib

    from pyspark.sql import functions as F  # deferred

    row = df.select(F.max(F.col(date_column)).alias("mx")).collect()[0]
    newest = row["mx"]
    if newest is None:
        return freshness(f"{layer}.freshness", None, max_lag_days, severity)
    newest_d = newest if isinstance(newest, date) else date.fromisoformat(str(newest)[:10])
    as_of_d = date.fromisoformat(as_of_date[:10])
    lag = (as_of_d - newest_d).days
    return freshness(f"{layer}.freshness", lag, max_lag_days, severity)
