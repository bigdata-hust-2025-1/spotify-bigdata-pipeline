import os
import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Cho phép import package `common` / `spark_jobs` ở gốc repo khi chạy spark-submit.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.config import TOPIC_PLAYBACK, checkpoint_location  # noqa: E402
from spark_jobs.stream.event_schema import PLAYBACK_EVENT_SCHEMA  # noqa: E402

# Cấu hình Kafka & Elasticsearch
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = TOPIC_PLAYBACK  # Tên topic thống nhất từ common.config (finding A2)
# Cấu hình ES
ES_NODES = "elasticsearch.bigdata"
ES_PORT = "9200"
# Typeless index — ES 7+ removed mapping types, so no legacy type suffix on the
# resource. Documents are keyed by event_id so a checkpoint replay upserts in
# place instead of creating duplicates.
ES_RESOURCE = "realtime_events"

JOB_NAME = "stream_processing_es"
WATERMARK_DELAY = "10 minutes"


def transform(df_kafka):
    """Parse Kafka bytes, deduplicate on event_id within the watermark, and
    add the derived columns the dashboard reads."""
    parsed = (
        df_kafka
        .select(
            F.from_json(
                F.col("value").cast("string"), PLAYBACK_EVENT_SCHEMA
            ).alias("data")
        )
        .select("data.*")
        .withColumn("event_time", F.timestamp_seconds(F.col("timestamp")))
        .withWatermark("event_time", WATERMARK_DELAY)
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    return (
        parsed
        .withColumn("processed_timestamp", F.current_timestamp())
        .withColumn(
            "is_completed",
            F.when(F.col("status") == "completed", 1).otherwise(0),
        )
    )


def main():
    spark = SparkSession.builder.appName("Spotify_ELK_Stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    df_clean = transform(df_kafka)

    print("--> Streaming to Elasticsearch...")
    query = (
        df_clean.writeStream
        .format("org.elasticsearch.spark.sql")
        # Durable checkpoint on shared object storage (never a local temp dir)
        # so a restart resumes from the last committed Kafka offsets.
        .option("checkpointLocation", checkpoint_location(JOB_NAME))
        .option("es.nodes", ES_NODES)
        .option("es.port", ES_PORT)
        .option("es.resource", ES_RESOURCE)
        .option("es.mapping.id", "event_id")
        .option("es.nodes.wan.only", "true")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
