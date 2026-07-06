"""Tests for PR-12 — Spark-on-K8s batch orchestration.

The SparkApplication specs are Jinja-rendered by ``SparkKubernetesOperator`` at
submit time, so here we render the same template variables and assert the
resulting spec is correct: named per-run (backfill-safe), carries the logical
date, and pulls MinIO credentials from a Secret rather than a hardcoded value.
A DagBag import test validates the DAG wiring when Airflow is installed (CI); it
skips locally where Airflow is absent.

Runnable via pytest or ``python tests/test_batch_pipeline.py`` (needs only PyYAML).
"""

import os
import sys
# unittest.SkipTest is recognised as a skip by both pytest (CI) and the
# standalone _run_all() runner, so tests skip cleanly in either.
from unittest import SkipTest as _Skip

import yaml

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_DAGS_DIR = os.path.join(_REPO_ROOT, "dags")
_YAML_DIR = os.path.join(_REPO_ROOT, "spark_jobs", "batch", "yaml")
if _DAGS_DIR not in sys.path:
    sys.path.insert(0, _DAGS_DIR)

_SAMPLE_DS = "2026-07-06"
_SAMPLE_DS_NODASH = "20260706"

_BATCH_SPECS = {
    "run_bronze_to_silver.yaml": "batch-bronze-to-silver",
    "run_silver_to_gold.yaml": "batch-silver-to-gold",
    "run_gold_to_es.yaml": "batch-gold-to-es",
    "run_maintenance.yaml": "batch-maintenance",
}


def _render(path):
    """Render the Jinja vars the operator would substitute, return (text, dict)."""
    with open(path, encoding="utf-8") as fh:
        raw = fh.read()
    rendered = raw.replace("{{ ds_nodash }}", _SAMPLE_DS_NODASH).replace(
        "{{ ds }}", _SAMPLE_DS
    )
    return rendered, yaml.safe_load(rendered)


def _env_map(container):
    """Return {name: entry} for a driver/executor env list."""
    return {e["name"]: e for e in container.get("env", [])}


def test_every_step_has_a_spec():
    for fname in _BATCH_SPECS:
        assert os.path.exists(os.path.join(_YAML_DIR, fname)), f"missing {fname}"


def test_specs_render_to_valid_sparkapplications():
    for fname, base in _BATCH_SPECS.items():
        _, doc = _render(os.path.join(_YAML_DIR, fname))
        assert doc["kind"] == "SparkApplication", f"{fname} wrong kind"
        # Per-run unique name so backfilling several dates never collides.
        assert doc["metadata"]["name"] == f"{base}-{_SAMPLE_DS_NODASH}", (
            f"{fname} name not per-run: {doc['metadata']['name']}"
        )


def test_logical_date_flows_into_every_container():
    for fname in _BATCH_SPECS:
        _, doc = _render(os.path.join(_YAML_DIR, fname))
        for role in ("driver", "executor"):
            env = _env_map(doc["spec"][role])
            assert "DT" in env, f"{fname} {role} missing DT env"
            assert env["DT"]["value"] == _SAMPLE_DS, (
                f"{fname} {role} DT is not the logical date"
            )


def test_minio_credentials_come_from_secret_not_plaintext():
    for fname in _BATCH_SPECS:
        raw, doc = _render(os.path.join(_YAML_DIR, fname))
        # The remediated secret must not reappear anywhere in the spec.
        assert "miniopass123" not in raw, f"{fname} still has a hardcoded secret"
        for role in ("driver", "executor"):
            env = _env_map(doc["spec"][role])
            for key in ("MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"):
                entry = env[key]
                assert "value" not in entry, f"{fname} {role} {key} is plaintext"
                ref = entry["valueFrom"]["secretKeyRef"]
                assert ref["name"] == "minio-credentials", (
                    f"{fname} {role} {key} wrong secret name"
                )


def test_batch_dag_structure():
    try:
        from airflow.models import DagBag
    except ModuleNotFoundError:
        raise _Skip("airflow not installed")
    dagbag = DagBag(dag_folder=_DAGS_DIR, include_examples=False)
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"
    dag = dagbag.get_dag("spotify_batch_pipeline")
    assert dag is not None, "spotify_batch_pipeline DAG not found"
    assert dag.catchup is True, "batch DAG must be backfillable (catchup=True)"
    assert dag.max_active_runs == 1, "backfills must be serialised"
    assert dag.default_args.get("depends_on_past") is False
    task_ids = set(dag.task_ids)
    expected = {
        "crawl_spotify", "land_bronze",
        "submit_bronze_to_silver", "wait_bronze_to_silver",
        "submit_silver_to_gold", "wait_silver_to_gold",
        "submit_gold_to_es", "wait_gold_to_es",
        "submit_maintenance", "wait_maintenance",
    }
    assert expected <= task_ids, f"missing tasks: {expected - task_ids}"
    # Each Spark step is waited on before the next is submitted.
    wait_silver = dag.get_task("wait_bronze_to_silver")
    assert "submit_silver_to_gold" in wait_silver.downstream_task_ids


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
