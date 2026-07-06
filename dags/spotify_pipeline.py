# dags/spotify_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import the task entrypoints. These modules build no clients and open no
# connections at import time (PR-11), so the scheduler can parse this DAG
# repeatedly without side effects.
from tasks.crawl_spotify import run_crawl
from tasks.kafka_to_minio import run_consume

default_args = {
    'owner': 'bigdata-team',
    # depends_on_past removed (was True): a failed or still-running prior run
    # must not silently block every future run — combined with retries that
    # could deadlock the schedule. Overlap is prevented by max_active_runs=1.
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spotify_pipeline',
    default_args=default_args,
    description='Crawl Spotify new releases → Kafka → MinIO (bronze)',
    # Runs every 7 minutes. (The previous comment claimed "daily", which
    # contradicted this cron; the schedule below is the source of truth.)
    schedule_interval='*/7 * * * *',
    start_date=datetime(2025, 12, 22),
    catchup=False,
    max_active_runs=1,
    tags=['spotify', 'bronze', 'ingestion'],
) as dag:

    crawl = PythonOperator(
        task_id='crawl_spotify',
        python_callable=run_crawl,
        # Thread the run's logical date ({{ ds }}) into the task instead of a
        # hardcoded literal, so backfills/reruns are deterministic.
        op_kwargs={'logical_date': '{{ ds }}'},
    )

    consume = PythonOperator(
        task_id='consume_to_minio',
        python_callable=run_consume,
        op_kwargs={'logical_date': '{{ ds }}'},
    )

    crawl >> consume  # Thứ tự: crawl xong mới chạy consume
