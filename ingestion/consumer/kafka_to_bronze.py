# ingestion/kafka_to_bronze.py
from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error
import json
import io
from datetime import date, datetime, timedelta
import os
import time
import signal
import sys

print("Consumer start")

def flush_buffer(topic: str):
    if not buffers[topic]:
        return

    domain = topic.replace("spotify_", "")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    object_name = f"{domain}/ingest_date={ingest_date}/{timestamp}.jsonl"

    data_bytes = "".join(buffers[topic]).encode('utf-8')

    try:
        minio_client.put_object(
            BRONZE_BUCKET, object_name,
            io.BytesIO(data_bytes), len(data_bytes),
            content_type="application/json"
        )
        print(f"Flushed {len(buffers[topic])} records → {object_name}")
        buffers[topic].clear()
    except S3Error as err:
        print(f"[MinIO Error] {err}")
        # Không clear → retry

# Config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPICS = ["spotify_albums", "spotify_tracks", "spotify_artists"]
GROUP_ID = "bronze-consumer-group-v1"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "miniopass123")  # Sửa: để env hoàn toàn
BRONZE_BUCKET = "spotify-bronze"

TEST_MODE = os.getenv("TEST_MODE", "0") == "1"

# Khởi tạo client
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
)

minio_client = Minio(MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET, secure=False)

if not minio_client.bucket_exists(BRONZE_BUCKET):
    minio_client.make_bucket(BRONZE_BUCKET)
    print(f"Created bucket {BRONZE_BUCKET}")

# Biến ngày (riêng cho test mode)
if TEST_MODE:
    ingest_date = date.today().isoformat()
    print("🚀 TEST_MODE = ON: Mỗi lần có dữ liệu → mô phỏng qua ngày mới")
else:
    ingest_date = date.today().isoformat()

buffers = {topic: [] for topic in TOPICS}
FLUSH_SIZE = 20

print("Kafka → MinIO Bronze consumer started...\n")

# Graceful shutdown
def signal_handler(sig, frame):
    print("\n🛑 Đang dừng consumer... Flushing buffer cuối cùng...")
    for topic in TOPICS:
        flush_buffer(topic)
    consumer.close()
    print("Đã dừng sạch sẽ. Bye!")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

while True:
    try:
        print(f"[DEBUG] Polling Kafka...")  # Thêm dòng này
        msg_pack = consumer.poll(timeout_ms=5000)
        print(f"[DEBUG] Poll result: {len(msg_pack)} topics with messages")  # Thêm dòng này

        has_new_data = bool(msg_pack)

        if not has_new_data:
            print("[DEBUG] No new data, checking buffers...")  # Thêm
            for topic in TOPICS:
                if buffers[topic]:
                    flush_buffer(topic)
            continue

        # Logic đổi ngày
        if TEST_MODE:
            # Mô phỏng: có data mới → qua ngày mới
            new_date = (datetime.strptime(ingest_date, "%Y-%m-%d") + timedelta(days=1)).date().isoformat()
            if has_new_data and new_date != ingest_date:
                print(f"🗓️  [TEST MODE] Chuyển ngày mô phỏng: {ingest_date} → {new_date}")
                for topic in TOPICS:
                    flush_buffer(topic)
                ingest_date = new_date
        else:
            current_date = date.today().isoformat()
            if current_date != ingest_date:
                print(f"🗓️  Chuyển ngày thực tế: {ingest_date} → {current_date}")
                for topic in TOPICS:
                    flush_buffer(topic)
                ingest_date = current_date

        # Xử lý message
        for tp, messages in msg_pack.items():
            topic = tp.topic
            for message in messages:
                data = json.loads(message.value)
                buffers[topic].append(json.dumps(data) + "\n")

                if len(buffers[topic]) >= FLUSH_SIZE:
                    flush_buffer(topic)

        consumer.commit()

    except Exception as e:
        print(f"[Error] {e}")
        time.sleep(10)

