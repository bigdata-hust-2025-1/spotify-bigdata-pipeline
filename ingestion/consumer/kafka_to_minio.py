# consumer/kafka_to_minio.py
import json
import os
import io
from datetime import datetime, timezone
from datetime import timedelta
from kafka import KafkaConsumer
from kafka import TopicPartition
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

load_dotenv()

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

# Khởi tạo MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Đảm bảo bucket tồn tại
try:
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' created successfully.")
    else:
        print(f"Bucket '{BUCKET_NAME}' already exists.")
except S3Error as e:
    print(f"Error checking/creating bucket: {e}")

# Hàm consume và upload (tự động commit offset)
def consume_and_upload(topic, folder):
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
            timestamp = data.get("timestamp", datetime.now(timezone.utc).isoformat())
            # Chuyển thành datetime object
            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

            # Cộng 1 ngày
            dt_plus_one_day = dt + timedelta(days=2)

            # Format lại thành string yyyy-mm-dd
            date_str = dt_plus_one_day.strftime("%Y-%m-%d")

            item_id = data.get("id", f"unknown_{message.offset}")
            object_name = f"{folder}/{date_str}/{item_id}.json"

            json_bytes = json.dumps(data).encode('utf-8')
            json_stream = io.BytesIO(json_bytes)

            minio_client.put_object(
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

# Chạy cho từng topic
print("Kafka to MinIO consumer started (chỉ message mới)...")
for topic, folder in TOPICS.items():
    consume_and_upload(topic, folder)

print("Tất cả topics đã xử lý xong!")