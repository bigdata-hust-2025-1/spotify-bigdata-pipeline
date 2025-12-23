from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

WORKLOAD_NS = "bigdata"
IMAGE_INGESTION = "spotify-ingestion:latest"

KAFKA_BOOTSTRAP = "{{ var.value.KAFKA_BOOTSTRAP_SERVERS if var.value.KAFKA_BOOTSTRAP_SERVERS else 'kafka-0.kafka.bigdata.svc.cluster.local:9092' }}"
MINIO_ENDPOINT = "{{ var.value.MINIO_ENDPOINT if var.value.MINIO_ENDPOINT else 'minio.bigdata.svc.cluster.local:9000' }}"
MINIO_BUCKET = "datalake"

default_args = {
    "owner": "bigdata-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="spotify_ingestion_bronze",
    description="Spotify → Kafka → MinIO (Bronze)",
    start_date=datetime(2025, 12, 22),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["spotify", "bronze", "ingestion"],
) as dag:

    producer_spotify_to_kafka = KubernetesPodOperator(
        task_id="producer_spotify_to_kafka",
        name="producer-spotify-to-kafka",
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

    consumer_kafka_to_minio_bronze = KubernetesPodOperator(
        task_id="consumer_kafka_to_minio_bronze",
        name="consumer-kafka-to-minio-bronze",
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
            "MAX_EMPTY_POLLS": "10",
            "POLL_TIMEOUT_SECONDS": "5",
        },
        get_logs=True,
        is_delete_operator_pod=False,
        execution_timeout=timedelta(minutes=15),
        log_events_on_failure=True,
    )

#     validate_bronze = KubernetesPodOperator(
#         task_id="validate_bronze",
#         name="validate-bronze",
#         namespace=WORKLOAD_NS,
#         image=IMAGE_INGESTION,
#         image_pull_policy="IfNotPresent",
#         cmds=["python", "-c"],
#         arguments=[r"""
# import os
# from minio import Minio

# endpoint = os.environ["MINIO_ENDPOINT"]
# bucket = os.environ["MINIO_BUCKET"]
# ak = os.environ["MINIO_ACCESS_KEY"]
# sk = os.environ["MINIO_SECRET_KEY"]
# dt = os.environ["DT"]

# client = Minio(endpoint, access_key=ak, secret_key=sk, secure=False)

# # Prefix tổng; bên dưới kiểm tra có object chứa dt=YYYY-MM-DD
# prefix = "bronze/spotify/"
# objs = list(client.list_objects(bucket, prefix=prefix, recursive=True))

# ok = any(f"dt={dt}" in o.object_name for o in objs)
# print(f"[VALIDATE] bucket={bucket} prefix={prefix} dt={dt} total_objs={len(objs)} ok={ok}")

# if not ok:
#     raise SystemExit(1)
#         """],
#         env_vars={
#             "DT": "{{ ds }}",
#             "MINIO_ENDPOINT": MINIO_ENDPOINT,
#             "MINIO_BUCKET": MINIO_BUCKET,
#             "MINIO_ACCESS_KEY": "{{ var.value.MINIO_ACCESS_KEY }}",
#             "MINIO_SECRET_KEY": "{{ var.value.MINIO_SECRET_KEY }}",
#         },
#         get_logs=True,
#         is_delete_operator_pod=True,
#         execution_timeout=timedelta(minutes=5),
#     )

    producer_spotify_to_kafka >> consumer_kafka_to_minio_bronze  #>> validate_bronze
