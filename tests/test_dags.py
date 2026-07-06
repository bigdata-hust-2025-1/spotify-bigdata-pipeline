"""Tests for PR-11 Airflow hygiene.

They verify the two ingestion task modules (``dags/tasks/*``) no longer open
network connections at import time, and that ``run_crawl`` drives an injected
Kafka producer correctly and always closes the one it owns. A DAG-import test is
included but skips gracefully when Airflow is not installed (it runs in CI).

Runnable via pytest or ``python tests/test_dags.py`` — no Spark/Airflow/JVM
required for the core assertions.
"""

import os
import subprocess
import sys
import textwrap
# unittest.SkipTest is recognised as a skip by both pytest (CI) and the
# standalone _run_all() runner, so tests skip cleanly in either.
from unittest import SkipTest as _Skip

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_DAGS_DIR = os.path.join(_REPO_ROOT, "dags")
if _DAGS_DIR not in sys.path:
    sys.path.insert(0, _DAGS_DIR)


def _import_opens_no_socket(module):
    """Import ``module`` in a subprocess with sockets blocked.

    Returns "ok" if it imported with no socket created, or "skip:<name>" when a
    third-party dependency is missing (we cannot exercise the module here).
    Fails (non-zero exit + no sentinel) if any socket is constructed at import.
    """
    # Guard the actual network act (connect), not socket construction: replacing
    # the socket type wholesale breaks stdlib `ssl` (which does
    # `class SSLSocket(socket)`). A real subclass keeps subclassing working while
    # failing loudly if anything tries to open a connection during import.
    script = textwrap.dedent(
        f"""
        import socket, sys
        _RealSocket = socket.socket

        class _GuardSocket(_RealSocket):
            def connect(self, *a, **k):
                raise AssertionError("network connect at import time")

            def connect_ex(self, *a, **k):
                raise AssertionError("network connect at import time")

        socket.socket = _GuardSocket
        try:
            import {module}  # noqa: F401
        except ModuleNotFoundError as e:
            print("SKIP:" + (e.name or "?"))
            sys.exit(0)
        finally:
            socket.socket = _RealSocket
        print("OK")
        """
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = _DAGS_DIR + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True, env=env, cwd=_REPO_ROOT,
    )
    out = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        raise AssertionError(f"import of {module} failed / opened a socket:\n{out}")
    if "SKIP:" in out:
        return "skip"
    if "OK" not in out:
        raise AssertionError(f"unexpected import result for {module}:\n{out}")
    return "ok"


def test_crawl_spotify_no_socket_at_import():
    # spotipy + kafka are importable here, so this genuinely proves the worst
    # offender (Spotify client + KafkaProducer + admin) is no longer built at
    # import time.
    if _import_opens_no_socket("tasks.crawl_spotify") == "skip":
        raise _Skip("crawl_spotify dependencies unavailable")


def test_kafka_to_minio_no_socket_at_import():
    if _import_opens_no_socket("tasks.kafka_to_minio") == "skip":
        raise _Skip("kafka_to_minio dependencies unavailable (minio)")


class _FakeProducer:
    def __init__(self):
        self.sent = []
        self.flushed = False
        self.closed = False

    def send(self, topic, item):
        self.sent.append((topic, item))

    def flush(self):
        self.flushed = True

    def close(self):
        self.closed = True


def _load_crawl_module():
    try:
        import tasks.crawl_spotify as mod
    except ModuleNotFoundError:
        raise _Skip("crawl_spotify dependencies unavailable")
    return mod


def test_run_crawl_publishes_all_and_owns_producer():
    mod = _load_crawl_module()
    # Neutralise the network-touching helpers.
    mod.create_topic_if_not_exists = lambda *a, **k: None
    mod.crawl_new_releases = lambda sp=None: (
        [{"id": "al"}], [{"id": "tr"}], [{"id": "ar"}, {"id": "ar2"}]
    )

    prod = _FakeProducer()
    sent = mod.run_crawl(logical_date="2026-07-06", sp=object(), producer=prod)

    assert sent == 4, f"expected 4 items sent, got {sent}"
    assert prod.flushed is True
    topics = {t for t, _ in prod.sent}
    assert topics == {
        mod.KAFKA_TOPIC_ALBUMS, mod.KAFKA_TOPIC_TRACKS, mod.KAFKA_TOPIC_ARTISTS
    }
    # An injected producer is NOT closed by run_crawl (caller owns it).
    assert prod.closed is False


def test_run_crawl_closes_producer_it_builds_even_on_error():
    mod = _load_crawl_module()
    mod.create_topic_if_not_exists = lambda *a, **k: None

    prod = _FakeProducer()
    mod.build_producer = lambda: prod
    mod.build_spotify = lambda: object()

    def _boom(sp=None):
        raise RuntimeError("crawl failed")

    mod.crawl_new_releases = _boom

    try:
        mod.run_crawl(logical_date="2026-07-06")
    except RuntimeError:
        pass
    else:
        raise AssertionError("run_crawl should propagate the crawl error")
    # Producer it created must be closed despite the failure (no leaked conn).
    assert prod.closed is True


def test_dag_imports_without_errors():
    try:
        from airflow.models import DagBag
    except ModuleNotFoundError:
        raise _Skip("airflow not installed")
    dagbag = DagBag(dag_folder=_DAGS_DIR, include_examples=False)
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"
    dag = dagbag.get_dag("spotify_pipeline")
    assert dag is not None, "spotify_pipeline DAG not found"
    assert dag.default_args.get("depends_on_past") is False


def _run_all():
    tests = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    failures = []
    skipped = 0
    for t in tests:
        try:
            t()
            print(f"PASS - {t.__name__}")
        except _Skip as exc:
            print(f"SKIP - {t.__name__}: {exc}")
            skipped += 1
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL - {t.__name__}: {exc}")
            failures.append(t.__name__)
    print()
    if failures:
        print(f"{len(failures)} FAILED: {', '.join(failures)}")
        return 1
    print(f"ALL {len(tests) - skipped} TESTS PASSED ({skipped} skipped)")
    return 0


if __name__ == "__main__":
    raise SystemExit(_run_all())
