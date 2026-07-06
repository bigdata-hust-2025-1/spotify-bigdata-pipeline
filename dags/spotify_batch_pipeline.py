"""Batch lakehouse pipeline orchestrated on Kubernetes (PR-12).

Heavy work must not run inside the Airflow worker (finding E3) and the Spark
batch ETL must not be orphaned from a schedule (finding F2). This DAG submits
every heavy step to Kubernetes and *waits* on it, so the pod's success/failure is
reflected as Airflow task state:

    crawl (pod) -> land bronze (pod)
      -> bronze_to_silver (SparkApplication) -> silver_to_gold (SparkApplication)
      -> gold_to_es (SparkApplication)        -> maintenance (SparkApplication)

Each Spark step is an operator that submits a ``SparkApplication`` plus a sensor
that blocks until the driver terminates. The run's logical date (``{{ ds }}``)
flows into every step, and each ``SparkApplication`` is named per-run
(``…-{{ ds_nodash }}``) so a backfill of several dates never collides. All steps
are idempotent (Iceberg MERGE / createOrReplace from PR-07/08), so re-running or
back-filling a date produces correct, non-duplicated data.

This is a *separate* ``dag_id`` from the legacy ``spotify_pipeline``; that DAG
stays until an explicit cutover, so this file can be reverted with no shared
state (rollback-safe).
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)

# --- Configuration (overridable via env on the scheduler) --------------------
WORKLOAD_NS = os.getenv("SPARK_NAMESPACE", "bigdata")
IMAGE_INGESTION = os.getenv("INGESTION_IMAGE", "spotify-ingestion:latest")
# Directory (mounted on the scheduler) holding the SparkApplication specs. The
# operator renders these files as Jinja templates before submitting them.
SPARK_YAML_DIR = os.getenv(
    "SPARK_YAML_DIR", "/opt/airflow/dags/repo/spark_jobs/batch/yaml"
)

KAFKA_BOOTSTRAP = (
    "{{ var.value.get('KAFKA_BOOTSTRAP_SERVERS', "
    "'kafka-0.kafka.bigdata.svc.cluster.local:9092') }}"
)
MINIO_ENDPOINT = (
    "{{ var.value.get('MINIO_ENDPOINT', 'minio.bigdata.svc.cluster.local:9000') }}"
)
MINIO_BUCKET = "datalake"

default_args = {
    "owner": "bigdata-team",
    # No depends_on_past: idempotent steps make each run independent; overlap is
    # prevented by max_active_runs=1 so backfills run one date at a time.
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def _spark_step(dag, task_id, yaml_file):
    """A Spark-on-K8s step: submit the SparkApplication, then sense completion.

    Returns ``(submit, sensor)``; wire ``submit >> sensor`` and chain sensors.
    The submitted application is named ``<name>-{{ ds_nodash }}`` inside the YAML,
    so the sensor watches that exact per-run application.
    """
    submit = SparkKubernetesOperator(
        task_id=f"submit_{task_id}",
        namespace=WORKLOAD_NS,
        application_file=os.path.join(SPARK_YAML_DIR, yaml_file),
        do_xcom_push=True,
        dag=dag,
    )
    sensor = SparkKubernetesSensor(
        task_id=f"wait_{task_id}",
        namespace=WORKLOAD_NS,
        # Pull the submitted application's name from the operator's XCom so the
        # sensor tracks the exact per-run SparkApplication it created.
        application_name=(
            f"{{{{ task_instance.xcom_pull(task_ids='submit_{task_id}')"
            "['metadata']['name'] }}"
        ),
        attach_log=True,
        poke_interval=30,
        timeout=60 * 60,
        mode="reschedule",
        dag=dag,
    )
    submit >> sensor
    return submit, sensor


with DAG(
    dag_id="spotify_batch_pipeline",
    default_args=default_args,
    description="Crawl -> bronze -> Iceberg silver/gold -> ES + maintenance (on K8s)",
    schedule_interval="@daily",
    start_date=datetime(2025, 12, 22),
    # Backfillable: catchup on so a re-enabled DAG (or `airflow dags backfill`)
    # replays missed dates; max_active_runs=1 serialises them (idempotent, but
    # this avoids write contention on the same Iceberg tables).
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "batch", "lakehouse", "iceberg"],
) as dag:

    crawl = KubernetesPodOperator(
        task_id="crawl_spotify",
        name="batch-crawl-spotify",
        namespace=WORKLOAD_NS,
        image=IMAGE_INGESTION,
        image_pull_policy="IfNotPresent",
        cmds=["python", "/app/ingestion/producer/crawl_spotify.py"],
        env_vars={
            "RUN_ONCE": "true",
            "DT": "{{ ds }}",
            "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP,
            "SPOTIFY_CLIENT_ID": "{{ var.value.SPOTIFY_CLIENT_ID }}",
            "SPOTIFY_CLIENT_SECRET": "{{ var.value.SPOTIFY_CLIENT_SECRET }}",
        },
        get_logs=True,
        is_delete_operator_pod=True,
        execution_timeout=timedelta(minutes=10),
    )

    land_bronze = KubernetesPodOperator(
        task_id="land_bronze",
        name="batch-land-bronze",
        namespace=WORKLOAD_NS,
        image=IMAGE_INGESTION,
        image_pull_policy="IfNotPresent",
        cmds=["python", "/app/ingestion/consumer/kafka_to_minio.py"],
        env_vars={
            "RUN_ONCE": "true",
            "DT": "{{ ds }}",
            "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP,
            "MINIO_ENDPOINT": MINIO_ENDPOINT,
            "MINIO_BUCKET": MINIO_BUCKET,
            "MINIO_ACCESS_KEY": "{{ var.value.MINIO_ACCESS_KEY }}",
            "MINIO_SECRET_KEY": "{{ var.value.MINIO_SECRET_KEY }}",
        },
        get_logs=True,
        is_delete_operator_pod=True,
        execution_timeout=timedelta(minutes=15),
    )

    # Spark ETL, each submitted to K8s and waited on before the next is submitted.
    b2s_submit, b2s_wait = _spark_step(dag, "bronze_to_silver", "run_bronze_to_silver.yaml")
    s2g_submit, s2g_wait = _spark_step(dag, "silver_to_gold", "run_silver_to_gold.yaml")
    g2e_submit, g2e_wait = _spark_step(dag, "gold_to_es", "run_gold_to_es.yaml")
    mnt_submit, _ = _spark_step(dag, "maintenance", "run_maintenance.yaml")

    crawl >> land_bronze >> b2s_submit
    b2s_wait >> s2g_submit
    s2g_wait >> g2e_submit
    g2e_wait >> mnt_submit
