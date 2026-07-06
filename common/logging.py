"""Structured JSON logging + fail-loud helpers (PR-13).

Pure standard library (no pyspark / JVM), so this imports anywhere and is
unit-testable without a cluster. Jobs:

- get a JSON logger via :func:`get_logger` (one ``run_id`` per process),
- emit per-stage row-count / duration metrics via :func:`log_metrics` /
  :class:`stage_timer`,
- use :class:`FailureCollector` to isolate per-dataset failures while still
  exiting **non-zero** if any dataset failed (replaces the old
  ``except Exception: print(); return`` that made jobs exit 0 on partial
  failure — findings C5, H1).

Note on the module name: Python 3 uses absolute imports, so ``import logging``
inside this file resolves to the standard library, not this module. Callers use
the package-qualified ``from common.logging import ...``.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone

# One run id per process, shared by every logger created here. Airflow can pin it
# (e.g. PIPELINE_RUN_ID="{{ run_id }}") so all task logs correlate.
RUN_ID = os.getenv("PIPELINE_RUN_ID") or uuid.uuid4().hex[:12]

# LogRecord attributes we never copy into the JSON payload as "extras".
_RESERVED = set(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__
) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    """Render a ``LogRecord`` as a single-line JSON object.

    Any structured fields attached via ``logger.info(msg, extra={...})`` are
    merged into the payload, so metrics and context are machine-parseable.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "run_id": getattr(record, "run_id", RUN_ID),
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and key not in payload:
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a logger that emits JSON to stdout. Idempotent per ``name``.

    Re-calling with the same name does not stack duplicate handlers, so importing
    a job module repeatedly (e.g. Airflow DAG parsing) stays clean.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    if not any(getattr(h, "_json_pipeline", False) for h in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        handler._json_pipeline = True  # type: ignore[attr-defined]
        logger.addHandler(handler)
    return logger


def log_metrics(logger: logging.Logger, stage: str, **fields) -> None:
    """Emit a structured metrics record (row counts, durations, ...)."""
    logger.info("metrics", extra={"event": "metrics", "stage": stage, **fields})


class stage_timer:
    """Context manager that logs a stage's start/end, duration, and metrics.

    Stash counts on the yielded dict; they are emitted with the ``stage_end``
    record::

        with stage_timer(log, "tracks") as m:
            m["rows_in"] = df.count()
            ...
            m["rows_out"] = out.count()

    Never suppresses exceptions — a genuine failure still propagates.
    """

    def __init__(self, logger: logging.Logger, stage: str, **fields):
        self.logger = logger
        self.stage = stage
        self.fields = dict(fields)
        self.metrics: dict = {}

    def __enter__(self) -> dict:
        self._t0 = time.monotonic()
        self.logger.info(
            "stage_start",
            extra={"event": "stage_start", "stage": self.stage, **self.fields},
        )
        return self.metrics

    def __exit__(self, exc_type, exc, tb) -> bool:
        duration_ms = round((time.monotonic() - self._t0) * 1000, 1)
        record = {
            "event": "stage_end",
            "stage": self.stage,
            "status": "ok" if exc_type is None else "error",
            "duration_ms": duration_ms,
            **self.fields,
            **self.metrics,
        }
        if exc_type is None:
            self.logger.info("stage_end", extra=record)
        else:
            self.logger.error("stage_end", extra=record)
        return False  # do not swallow


class PipelineError(RuntimeError):
    """Raised at the end of a job when one or more stages failed."""


class FailureCollector:
    """Collect per-stage failures, then fail the whole job loudly.

    Enables per-dataset isolation (attempt every dataset) without the old bug of
    exiting 0 on partial failure: call :meth:`raise_if_any` at the end so any
    recorded failure raises :class:`PipelineError` (non-zero exit).
    """

    def __init__(self, logger: logging.Logger | None = None):
        self.failures: list[tuple[str, BaseException]] = []
        self.logger = logger

    def record(self, name: str, exc: BaseException) -> None:
        self.failures.append((name, exc))
        if self.logger is not None:
            self.logger.error(
                "stage_failed",
                extra={"event": "stage_failed", "stage": name, "error": str(exc)},
                exc_info=(type(exc), exc, exc.__traceback__),
            )

    def collect(self, name: str) -> "_Collecting":
        """Context manager capturing an exception for ``name`` (isolated here)."""
        return _Collecting(self, name)

    def raise_if_any(self) -> None:
        if self.failures:
            names = ", ".join(name for name, _ in self.failures)
            raise PipelineError(f"{len(self.failures)} stage(s) failed: {names}")


class _Collecting:
    def __init__(self, collector: FailureCollector, name: str):
        self.collector = collector
        self.name = name

    def __enter__(self) -> "_Collecting":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is not None:
            # Isolate the failure here; the job re-raises a summary at the end.
            self.collector.record(self.name, exc)
            return True
        return False
