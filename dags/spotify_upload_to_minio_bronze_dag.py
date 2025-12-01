from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

# ================== CONFIG ==================

PROJECT_ROOT = os.environ.get(
    "SPOTIFY_PIPELINE_ROOT",
    "/opt/airflow/spotify-bigdata-pipeline",
)

default_args = {
    "owner": "spotify_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="spotify_upload_to_minio_bronze",
    description="Upload crawled Spotify JSON files lên MinIO Bronze sau khi crawl DAG thành công",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    # schedule giống DAG crawl để cùng execution_date (hoặc @daily)
    schedule_interval="0 2 * * *",  # ví dụ 02:00 mỗi ngày
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "batch", "minio", "bronze"],
) as dag:

    # 1) Đợi DAG crawl xong & validate OK
    wait_for_crawl = ExternalTaskSensor(
        task_id="wait_for_crawl_and_validate",
        external_dag_id="spotify_crawl_metadata",
        external_task_id="validate_crawled_metadata",
        # dùng execution_date của chính DAG này để map sang DAG kia
        mode="poke",            # hoặc "reschedule" nếu muốn tiết kiệm slot
        poke_interval=60,       # 60s check 1 lần
        timeout=60 * 60 * 3,    # tối đa chờ 3h
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
    )

    # 2) Sau khi crawl + validate xong -> upload lên MinIO
    upload_to_minio_bronze = BashOperator(
        task_id="upload_to_minio_bronze",
        bash_command=f"cd {PROJECT_ROOT} && python minIO/upload_minio.py",
        env={
            # ép LOCAL_DATA_DIR cho script upload_minio.py
            "LOCAL_DATA_DIR": os.path.join(PROJECT_ROOT, "data"),
        },
    )

    wait_for_crawl >> upload_to_minio_bronze
