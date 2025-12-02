from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# ================== CONFIG ==================

PROJECT_ROOT = os.environ.get(
    "SPOTIFY_PIPELINE_ROOT",
    "/opt/airflow/spotify-bigdata-pipeline",  # chỉnh nếu khác
)

DATA_DIR = os.path.join(PROJECT_ROOT, "data")

default_args = {
    "owner": "spotify_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ================== VALIDATION FUNC ==================

def validate_crawled_files(**context):
    """
    Kiểm tra các file JSON sau khi crawl:
    - Có tồn tại không
    - File có size > 0 không
    Nếu thiếu / rỗng -> raise Exception để fail DAG.
    """
    expected_files = [
        "albums.json",
        "albums_processing_metadata.json",
        "artists.json",
        "owners.json",
        "playlists.json",
        "playlists_processing_metadata.json",
        "tracks.json",
        "tracks_processing_metadata.json",
    ]

    missing_or_empty = []

    for fname in expected_files:
        fpath = os.path.join(DATA_DIR, fname)
        if not os.path.exists(fpath):
            missing_or_empty.append(f"{fname} (không tồn tại)")
            continue

        if os.path.getsize(fpath) == 0:
            missing_or_empty.append(f"{fname} (size = 0)")

    if missing_or_empty:
        msg = "Các file crawl bị thiếu hoặc rỗng:\n" + "\n".join(
            f"- {x}" for x in missing_or_empty
        )
        raise ValueError(msg)

    print("✅ Tất cả file JSON sau crawl đều tồn tại và có dữ liệu.")


# ================== DAG DEFINITION ==================

with DAG(
    dag_id="spotify_crawl_metadata",
    description="Batch crawl Spotify metadata (owners, playlists, artists, albums, tracks) và validate output",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule_interval="0 1 * * *",  # 01:00 mỗi ngày
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "batch", "crawl"],
) as dag:

    # Crawl song song các loại metadata
    with TaskGroup(group_id="crawl_metadata") as crawl_group:

        crawl_owners = BashOperator(
            task_id="crawl_owners",
            bash_command=f"cd {PROJECT_ROOT} && python data/crawl_owners.py",
        )

        crawl_playlists = BashOperator(
            task_id="crawl_playlists",
            bash_command=f"cd {PROJECT_ROOT} && python data/crawl_playlist.py",
        )

        crawl_artists = BashOperator(
            task_id="crawl_artists",
            bash_command=f"cd {PROJECT_ROOT} && python data/crawl_artists.py",
        )

        crawl_albums = BashOperator(
            task_id="crawl_albums",
            bash_command=f"cd {PROJECT_ROOT} && python data/crawl_albums.py",
        )

        crawl_tracks = BashOperator(
            task_id="crawl_tracks",
            bash_command=f"cd {PROJECT_ROOT} && python data/crawl_tracks.py",
        )

        # Cho Airflow hiểu group này có các task con
        [crawl_owners, crawl_playlists, crawl_artists, crawl_albums, crawl_tracks]

    # Validate sau khi tất cả crawl xong
    validate_metadata = PythonOperator(
        task_id="validate_crawled_metadata",
        python_callable=validate_crawled_files,
        provide_context=True,
    )

    crawl_group >> validate_metadata
