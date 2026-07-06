"""Tests for the structured-logging + fail-loud helpers in ``common.logging`` (PR-13).

Pure stdlib — no Spark/JVM. Covers: logs are parseable JSON carrying a run id;
per-stage metrics fields are emitted; and a failure in one dataset causes the job
to exit non-zero (FailureCollector isolates then re-raises).

Runnable via pytest or ``python tests/test_logging.py``.
"""

import io
import json
import logging
import os
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.logging import (  # noqa: E402
    RUN_ID,
    FailureCollector,
    JsonFormatter,
    PipelineError,
    get_logger,
    log_metrics,
    stage_timer,
)


def _capturing_logger(name):
    """A logger wired to an in-memory JSON stream; returns (logger, get_lines)."""
    logger = logging.getLogger(name)
    for h in list(logger.handlers):
        logger.removeHandler(h)
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    def lines():
        return [json.loads(ln) for ln in buf.getvalue().splitlines() if ln.strip()]

    return logger, lines


def test_logs_are_valid_json_with_run_id():
    logger, lines = _capturing_logger("t.json")
    logger.info("hello")
    recs = lines()
    assert len(recs) == 1
    rec = recs[0]
    assert rec["msg"] == "hello"
    assert rec["level"] == "INFO"
    assert rec["run_id"] == RUN_ID and rec["run_id"]  # non-empty run id


def test_log_metrics_emits_stage_and_counts():
    logger, lines = _capturing_logger("t.metrics")
    log_metrics(logger, "tracks", rows_in=100, rows_out=95)
    rec = lines()[0]
    assert rec["event"] == "metrics"
    assert rec["stage"] == "tracks"
    assert rec["rows_in"] == 100 and rec["rows_out"] == 95


def test_stage_timer_emits_start_end_duration_and_metrics():
    logger, lines = _capturing_logger("t.timer")
    with stage_timer(logger, "albums") as m:
        m["rows_out"] = 42
    recs = lines()
    assert [r["event"] for r in recs] == ["stage_start", "stage_end"]
    end = recs[1]
    assert end["stage"] == "albums"
    assert end["status"] == "ok"
    assert end["rows_out"] == 42
    assert isinstance(end["duration_ms"], (int, float))


def test_stage_timer_marks_error_and_reraises():
    logger, lines = _capturing_logger("t.timer.err")
    try:
        with stage_timer(logger, "boom"):
            raise ValueError("kaboom")
    except ValueError:
        pass
    else:
        raise AssertionError("stage_timer must not swallow exceptions")
    end = lines()[-1]
    assert end["event"] == "stage_end" and end["status"] == "error"


def test_get_logger_is_idempotent():
    a = get_logger("t.idem")
    n = len(a.handlers)
    b = get_logger("t.idem")
    assert a is b
    assert len(b.handlers) == n  # no duplicate handler stacked


def test_failure_collector_isolates_then_raises_nonzero():
    logger, lines = _capturing_logger("t.fc")
    failures = FailureCollector(logger)
    processed = []
    for name in ("tracks", "albums", "artists"):
        with failures.collect(name):
            processed.append(name)
            if name == "albums":
                raise RuntimeError("albums failed")
    # Isolation: every dataset was attempted despite the middle failure.
    assert processed == ["tracks", "albums", "artists"]
    try:
        failures.raise_if_any()
    except PipelineError as exc:
        assert "albums" in str(exc)
        return
    raise AssertionError("raise_if_any must raise when a stage failed")


def test_failure_collector_silent_when_all_ok():
    failures = FailureCollector()
    for name in ("a", "b"):
        with failures.collect(name):
            pass
    failures.raise_if_any()  # must not raise


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
