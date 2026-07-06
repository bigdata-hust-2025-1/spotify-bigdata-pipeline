# consumer/kafka_to_minio.py
import json
import os
import io
from datetime import datetime, timezone
from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error
# from dotenv import load_dotenv

# load_dotenv()

# Config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")  # Sửa nếu dùng external
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_NAME = "spotify-bronze"

TOPICS = {
    "spotify_albums": "albums",
    "spotify_tracks": "tracks",
    "spotify_artists": "artists"
}

# NOTE (PR-11): the MinIO client and bucket check are built lazily inside the
# factories below — never at import time. Airflow re-parses this module every
# few seconds, so constructing a live client (or running the consume loop) at
# module scope opened network connections on every parse. run_consume() owns the
# client lifecycle instead.


def build_minio_client():
    """Construct the MinIO client. Fails fast if credentials are missing."""
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise EnvironmentError(
            "MINIO_ROOT_USER và MINIO_ROOT_PASSWORD phải được thiết lập!"
        )
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def ensure_bucket(client, bucket=BUCKET_NAME):
    """Create the bucket if it does not exist (idempotent)."""
    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"Bucket '{bucket}' created successfully.")
        else:
            print(f"Bucket '{bucket}' already exists.")
    except S3Error as e:
        print(f"Error checking/creating bucket: {e}")


# Hàm consume và upload (tự động commit offset)
def consume_and_upload(topic, folder, partition_date=None, client=None):
    if client is None:
        client = build_minio_client()
    ensure_bucket(client)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="spotify_to_minio_group",          # Group cố định để lưu offset
        auto_offset_reset='earliest',               # Lần đầu đọc từ đầu, sau đó từ offset đã commit
        enable_auto_commit=True,                    # Tự động commit
        auto_commit_interval_ms=5000,               # Commit mỗi 5 giây
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=30000                   # Timeout nếu không có message mới
    )

    print(f"Đang consume topic '{topic}' (từ offset đã commit) → folder: {folder}")
    count = 0
    batch_size = 10  # Commit thủ công sau mỗi 10 message (an toàn hơn)

    try:
        for message in consumer:
            data = message.value
            if partition_date:
                # Airflow supplies the run's logical date ({{ ds }}); partition
                # bronze objects by it so backfills land deterministically.
                date_str = partition_date
            else:
                # Standalone fallback: derive the partition from the record's own
                # timestamp when no logical date is provided.
                timestamp = data.get("timestamp", datetime.now(timezone.utc).isoformat())
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d")

            item_id = data.get("id", f"unknown_{message.offset}")
            object_name = f"{folder}/{date_str}/{item_id}.json"

            json_bytes = json.dumps(data).encode('utf-8')
            json_stream = io.BytesIO(json_bytes)

            client.put_object(
                BUCKET_NAME,
                object_name,
                json_stream,
                length=len(json_bytes)
            )
            count += 1
            print(f"Uploaded: {object_name}")

            # Commit thủ công sau mỗi batch để tránh mất message nếu crash
            if count % batch_size == 0:
                consumer.commit()
                print(f"Committed offset sau {count} messages")

    except Exception as e:
        print(f"Lỗi khi xử lý topic {topic}: {e}")
    finally:
        # Commit lần cuối trước khi đóng
        consumer.commit()
        consumer.close()
        print(f"Hoàn thành topic '{topic}' - Tổng: {count} messages mới")

# ==================== AIRFLOW TASK ENTRYPOINT ====================
def run_consume(logical_date=None, client=None):
    """Consume every topic and upload records to MinIO under ``logical_date``.

    Callable invoked by the Airflow ``consume_to_minio`` task. Builds a single
    MinIO client (unless one is injected for tests), ensures the bucket, and
    fans out over the topics. Nothing runs at import time (PR-11); the run's
    ``{{ ds }}`` flows in as ``logical_date`` and drives the object partition.
    """
    if client is None:
        client = build_minio_client()
    ensure_bucket(client)

    print(f"[consume] logical_date={logical_date} - Kafka to MinIO started...")
    for topic, folder in TOPICS.items():
        consume_and_upload(topic, folder, partition_date=logical_date, client=client)
    print("Tất cả topics đã xử lý xong!")
