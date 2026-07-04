"""Unit tests for :mod:`common.config`.

Runnable two ways:
  * ``python -m pytest tests/test_config.py`` (once pytest lands in PR-14), or
  * ``python tests/test_config.py`` (no third-party deps required).

Covers the module's design contract: non-empty constants, env-override
precedence, ``get_ingest_date`` behaviour, and — most importantly — that
importing ``common.config`` performs no network I/O (subprocess-isolated).
"""

import os
import subprocess
import sys

# Make the repo root importable regardless of the working directory.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import common.config as config  # noqa: E402  (import after sys.path bootstrap)


def test_topic_constants_are_nonempty_strings():
    for name in ("TOPIC_PLAYBACK", "TOPIC_ANOMALY",
                 "TOPIC_ALBUMS", "TOPIC_TRACKS", "TOPIC_ARTISTS"):
        value = getattr(config, name)
        assert isinstance(value, str) and value.strip(), f"{name} must be non-empty"


def test_bucket_and_endpoint_constants_are_nonempty_strings():
    for name in ("BRONZE_BUCKET", "SILVER_BUCKET", "GOLD_BUCKET",
                 "KAFKA_BOOTSTRAP_SERVERS", "MINIO_ENDPOINT",
                 "ES_NODES", "ES_PORT", "CASSANDRA_HOST", "CASSANDRA_PORT"):
        value = getattr(config, name)
        assert isinstance(value, str) and value.strip(), f"{name} must be non-empty"


def test_playback_default_preserves_existing_topic_name():
    # Behaviour-preserving: default must remain the historical working value.
    if not os.getenv("TOPIC_PLAYBACK"):
        assert config.TOPIC_PLAYBACK == "spotify_playback_events"


def test_get_ingest_date_default():
    saved = os.environ.pop("INGEST_DATE", None)
    try:
        assert config.get_ingest_date() == config.DEFAULT_INGEST_DATE == "2025-12-21"
    finally:
        if saved is not None:
            os.environ["INGEST_DATE"] = saved


def test_get_ingest_date_env_override_wins():
    saved = os.environ.get("INGEST_DATE")
    os.environ["INGEST_DATE"] = "2099-01-01"
    try:
        assert config.get_ingest_date() == "2099-01-01"
    finally:
        if saved is None:
            os.environ.pop("INGEST_DATE", None)
        else:
            os.environ["INGEST_DATE"] = saved


def test_checkpoint_location_is_durable_and_namespaced():
    path = config.checkpoint_location("stream_x")
    assert path == config.CHECKPOINT_ROOT.rstrip("/") + "/stream_x"
    assert "/tmp" not in path, "checkpoints must never live under /tmp"


def test_checkpoint_location_rejects_empty_job_name():
    for bad in ("", None):
        try:
            config.checkpoint_location(bad)  # type: ignore[arg-type]
        except ValueError:
            continue
        raise AssertionError(f"expected ValueError for job_name={bad!r}")


def test_checkpoint_root_env_override_and_trailing_slash():
    """A trailing slash on CHECKPOINT_ROOT must not produce a double slash."""
    env = dict(os.environ, CHECKPOINT_ROOT="s3a://cp/", PYTHONPATH=_REPO_ROOT)
    code = (
        "import common.config as c; "
        "p = c.checkpoint_location('j'); "
        "assert p == 's3a://cp/j', p; print('OK')"
    )
    out = subprocess.run([sys.executable, "-c", code], env=env,
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "OK" in out.stdout


def test_topic_env_override_wins_at_import():
    """A constant set via env BEFORE import must win (fresh subprocess)."""
    env = dict(os.environ, TOPIC_PLAYBACK="override.topic.v9", PYTHONPATH=_REPO_ROOT)
    code = (
        "import common.config as c; "
        "assert c.TOPIC_PLAYBACK == 'override.topic.v9', c.TOPIC_PLAYBACK; "
        "print('OK')"
    )
    out = subprocess.run([sys.executable, "-c", code], env=env,
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "OK" in out.stdout


def test_import_has_no_network_side_effects():
    """Importing common.config must not open any socket (subprocess-isolated)."""
    env = dict(os.environ, PYTHONPATH=_REPO_ROOT)
    code = (
        "import socket\n"
        "def _boom(*a, **k):\n"
        "    raise AssertionError('network I/O at import time')\n"
        "socket.socket = _boom\n"
        "socket.create_connection = _boom\n"
        "import common.config  # must not touch the network\n"
        "print('OK')\n"
    )
    out = subprocess.run([sys.executable, "-c", code], env=env,
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "OK" in out.stdout


def _run_all():
    tests = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    failures = []
    for t in tests:
        try:
            t()
            print(f"PASS - {t.__name__}")
        except Exception as exc:  # noqa: BLE001 - test runner surfaces all failures
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
