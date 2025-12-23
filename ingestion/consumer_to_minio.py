# ingestion/consumer_to_minio.py
import os
import json
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Tuple
from urllib.parse import urlparse

from kafka import KafkaConsumer, TopicPartition
from minio import Minio
from minio.error import S3Error


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        return default
    return v


def _parse_minio_endpoint(endpoint: str) -> Tuple[str, bool]:
    """
    Accepts:
      - "minio.bigdata.svc.cluster.local:9000"
      - "http://minio.bigdata.svc.cluster.local:9000"
      - "https://..."
    Returns: (host_port, secure)
    """
    ep = endpoint.strip()
    if ep.startswith("http://") or ep.startswith("https://"):
        u = urlparse(ep)
        host_port = u.netloc
        secure = (u.scheme == "https")
        return host_port, secure
    return ep, False


def _topic_to_entity(topic: str) -> str:
    # Your topics are spotify_albums / spotify_tracks / spotify_artists
    if topic.startswith("spotify_"):
        return topic.replace("spotify_", "", 1)  # albums/tracks/artists
    return topic


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _make_object_name(bucket_prefix: str, entity: str, ingest_date: str, part_idx: int) -> str:
    ts = _utc_now().strftime("%Y%m%dT%H%M%SZ")
    run_id = uuid.uuid4().hex[:8]
    # Keep your style: <entity>/ingest_date=YYYY-MM-DD/<entity>_<ts>_<run>_part-0001.jsonl
    fname = f"{entity}_{ts}_{run_id}_part-{part_idx:04d}.jsonl"
    # ensure no double slashes
    prefix = bucket_prefix.strip("/")

    return f"{prefix}/{entity}/ingest_date={ingest_date}/{fname}" if prefix else f"{entity}/ingest_date={ingest_date}/{fname}"


def _jsonl_bytes(records: List[dict]) -> bytes:
    # one record per line, UTF-8
    return ("\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n").encode("utf-8")


def ensure_bucket(minio: Minio, bucket: str) -> None:
    if not minio.bucket_exists(bucket):
        minio.make_bucket(bucket)


def main():
    # ===== Kafka =====
    kafka_bootstrap = _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topics = [t.strip() for t in _env("KAFKA_TOPICS", "spotify_albums,spotify_tracks,spotify_artists").split(",") if t.strip()]
    group_id = _env("KAFKA_GROUP_ID", "spotify-consumer-to-minio")
    auto_offset_reset = _env("KAFKA_AUTO_OFFSET_RESET", "earliest")  # earliest/latest

    # ===== MinIO =====
    minio_endpoint_raw = _env("MINIO_ENDPOINT", "minio:9000")  # may include http://
    minio_access = _env("MINIO_ACCESS_KEY")
    minio_secret = _env("MINIO_SECRET_KEY")
    minio_bucket = _env("MINIO_BUCKET", "spotify-bronze")
    bucket_prefix = _env("MINIO_PREFIX", "").strip("/")  # optional, e.g. "raw" if you want raw/<entity>/...

    # ===== Batch / runtime =====
    batch_size = int(_env("BATCH_SIZE", "500"))
    flush_seconds = int(_env("FLUSH_SECONDS", "10"))
    run_seconds = int(_env("RUN_SECONDS", "0"))  # 0 = run forever unless RUN_ONCE=true
    run_once = _env("RUN_ONCE", "false").lower() == "true"

    if not topics:
        raise RuntimeError("KAFKA_TOPICS is empty.")
    if not minio_access or not minio_secret:
        raise RuntimeError("MINIO_ACCESS_KEY / MINIO_SECRET_KEY must be set.")

    host_port, secure = _parse_minio_endpoint(minio_endpoint_raw)
    minio = Minio(host_port, access_key=minio_access, secret_key=minio_secret, secure=secure)

    # Create bucket if needed
    ensure_bucket(minio, minio_bucket)

    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=kafka_bootstrap,
        group_id=group_id,
        enable_auto_commit=False,          # commit only after successful upload
        auto_offset_reset=auto_offset_reset,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=5000,          # 5s timeout để có thời gian poll messages (tăng từ 1s)
        max_poll_records=1000,
    )

    buffers: Dict[str, List[dict]] = {t: [] for t in topics}
    # track offsets to commit per topic-partition (chỉ track offsets của topic đang flush)
    topic_offsets: Dict[str, Dict[TopicPartition, int]] = {t: {} for t in topics}
    last_flush = time.time()
    start_time = time.time()
    part_counter: Dict[str, int] = {t: 1 for t in topics}
    total_processed: Dict[str, int] = {t: 0 for t in topics}  # Track số records đã xử lý thành công

    def flush_topic(topic: str) -> None:
        records = buffers.get(topic, [])
        if not records:
            return

        entity = _topic_to_entity(topic)
        ingest_date = _utc_now().strftime("%Y-%m-%d")
        object_name = _make_object_name(bucket_prefix, entity, ingest_date, part_counter[topic])
        data = _jsonl_bytes(records)

        # Get offsets chỉ của topic này để commit
        offsets_to_commit = topic_offsets.get(topic, {}).copy()

        # Upload
        try:
            minio.put_object(
                bucket_name=minio_bucket,
                object_name=object_name,
                data=io_bytes(data),
                length=len(data),
                content_type="application/jsonl",
            )
        except S3Error as e:
            raise RuntimeError(f"MinIO put_object failed for {object_name}: {e}") from e

        # If upload succeeded, commit offsets for THIS topic's partitions only
        if offsets_to_commit:
            consumer.commit(offsets=offsets_to_commit)
            print(f"[OK] Committed offsets for {len(offsets_to_commit)} partitions of {topic}")

        # Clear buffer, offsets tracking & increment part index
        buffers[topic] = []
        topic_offsets[topic] = {}  # Clear offsets cho topic này
        total_processed[topic] += len(records)
        part_counter[topic] += 1
        print(f"[OK] Uploaded {len(records)} records from {topic} -> s3://{minio_bucket}/{object_name}")

    def flush_all() -> None:
        for t in topics:
            flush_topic(t)

    # Helper: MinIO put_object needs a stream-like object
    class io_bytes:
        def __init__(self, b: bytes):
            self._b = b
            self._i = 0

        def read(self, n: int = -1) -> bytes:
            if n == -1:
                n = len(self._b) - self._i
            chunk = self._b[self._i:self._i + n]
            self._i += len(chunk)
            return chunk

    print(f"[START] Kafka -> MinIO consumer (group={group_id})")
    print(f"        bootstrap={kafka_bootstrap}")
    print(f"        topics={topics}")
    print(f"        minio={minio_endpoint_raw} bucket={minio_bucket} prefix='{bucket_prefix}'")
    print(f"        batch_size={batch_size} flush_seconds={flush_seconds} run_once={run_once} run_seconds={run_seconds}")

    # Test Kafka connection and check subscription
    try:
        print(f"[INFO] Testing Kafka connection to {kafka_bootstrap}...")
        # Check subscribed topics
        subscribed = consumer.subscription()
        print(f"[INFO] ✅ Consumer subscribed to topics: {subscribed}")
        
        # Get partition info for subscribed topics
        partitions = consumer.assignment()
        print(f"[INFO] Consumer assigned partitions: {partitions}")
    except Exception as e:
        print(f"[WARNING] Could not verify subscription: {e}")
        print(f"[INFO] Continuing anyway...")

    try:
        consecutive_empty_polls = 0  # Track số lần poll không có messages
        max_empty_polls = 10  # Exit sau 10 lần poll rỗng liên tiếp (khi run_once=False)
        
        # Log thông tin ban đầu
        print(f"[INFO] Consumer started with run_once={run_once}, run_seconds={run_seconds}")
        print(f"[INFO] Will exit after {max_empty_polls if not run_once else 20} consecutive empty polls")
        
        while True:
            # Exit conditions
            if run_seconds > 0 and (time.time() - start_time) >= run_seconds:
                print(f"[INFO] RUN_SECONDS ({run_seconds}s) reached. Flushing and exiting.")
                print(f"[INFO] Total processed: {sum(total_processed.values())} records across all topics")
                flush_all()
                break

            polled_any = False
            poll_count = 0
            should_flush_time = False  # Flag để break outer loop khi cần time-based flush
            try:
                # Use poll() method for better control and timeout handling
                msg_pack = consumer.poll(timeout_ms=5000, max_records=1000)
                if msg_pack:
                    for tp, messages in msg_pack.items():
                        for msg in messages:
                            polled_any = True
                            poll_count += 1
                            topic = msg.topic
                            buffers[topic].append(msg.value)

                            # Track next offset to commit cho topic này (Kafka commit uses "next offset")
                            topic_offsets[topic][tp] = msg.offset + 1

                            # Flush per-topic when batch_size reached
                            if len(buffers[topic]) >= batch_size:
                                flush_topic(topic)

                            # Check time-based flush - dùng flag để break outer loop
                            if time.time() - last_flush >= flush_seconds:
                                should_flush_time = True
                                break
                        if should_flush_time:
                            break
            except Exception as e:
                # Log any errors during polling
                if "StopIteration" not in str(type(e).__name__):
                    print(f"[WARNING] Error during poll: {e}")
                # consumer_timeout_ms reached - no messages in this poll
                pass
            
            if polled_any:
                consecutive_empty_polls = 0  # Reset counter khi có messages
                if poll_count > 0:
                    print(f"[INFO] Polled {poll_count} messages in this iteration")
            else:
                consecutive_empty_polls += 1
                if consecutive_empty_polls % 5 == 0:  # Log mỗi 5 lần poll rỗng
                    print(f"[INFO] No messages polled (empty polls: {consecutive_empty_polls})")

            # time-based flush
            if time.time() - last_flush >= flush_seconds:
                last_flush = time.time()
                flush_all()

            if not polled_any:
                # no messages polled in this loop
                if run_once:
                    # Nếu run_once và không còn messages sau nhiều lần poll, flush và thoát
                    # Đợi thêm một chút để đảm bảo không còn messages mới
                    if consecutive_empty_polls >= 20:  # Tăng từ 10 lên 20 để đảm bảo consume hết
                        print(f"[INFO] RUN_ONCE=true: No messages for {consecutive_empty_polls} consecutive polls. Flushing and exiting.")
                        print(f"[INFO] Total processed: {sum(total_processed.values())} records across all topics")
                        flush_all()
                        break
                else:
                    # Nếu không có messages trong nhiều lần poll liên tiếp, có thể đã consume hết
                    if consecutive_empty_polls >= max_empty_polls:
                        print(f"[INFO] No messages for {max_empty_polls} consecutive polls. Flushing and exiting.")
                        flush_all()
                        break
                # Otherwise, still flush on schedule
                if time.time() - last_flush >= flush_seconds:
                    last_flush = time.time()
                    flush_all()

    except KeyboardInterrupt:
        print("[STOP] KeyboardInterrupt. Flushing before exit...")
        flush_all()
    finally:
        consumer.close()
        print("[DONE] consumer closed.")


if __name__ == "__main__":
    main()
