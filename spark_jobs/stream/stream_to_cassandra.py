import os
import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Cho phép import package `common` / `spark_jobs` ở gốc repo khi chạy spark-submit.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.config import (  # noqa: E402
    CASSANDRA_HOST,
    CASSANDRA_PORT,
    TOPIC_PLAYBACK,
    checkpoint_location,
)
from spark_jobs.stream.event_schema import (  # noqa: E402
    PLAYBACK_EVENT_SCHEMA,
    USER_PLAYS_COLUMNS,
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka.bigdata:9092")

# Định danh ổn định của query — dùng làm thư mục checkpoint. KHÔNG đổi tuỳ tiện.
JOB_NAME = "stream_to_cassandra"
CASSANDRA_KEYSPACE = "spotify_ks"
CASSANDRA_TABLE = "user_plays"
WATERMARK_DELAY = "10 minutes"


def build_spark():
    return (
        SparkSession.builder
        .appName("Stream_to_Cassandra")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", CASSANDRA_PORT)
        .getOrCreate()
    )


def transform(df_kafka):
    """Parse raw Kafka bytes into the typed ``user_plays`` column set.

    Steps: JSON-decode ``value`` with the playback schema, derive the
    event-time timestamp from the epoch field, then deduplicate on
    ``event_id`` within the watermark so a checkpoint replay does not double
    up rows, and finally project exactly the Cassandra table columns.
    """
    parsed = (
        df_kafka
        .select(
            F.from_json(
                F.col("value").cast("string"), PLAYBACK_EVENT_SCHEMA
            ).alias("e")
        )
        .select("e.*")
        .withColumn("event_time", F.timestamp_seconds(F.col("timestamp")))
        .withWatermark("event_time", WATERMARK_DELAY)
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    return parsed.select(*USER_PLAYS_COLUMNS)


def write_to_cassandra(batch_df, _epoch_id):
    (
        batch_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE)
        .save()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_PLAYBACK)
        .option("startingOffsets", "latest")
        .load()
    )

    df_out = transform(df_kafka)

    query = (
        df_out.writeStream
        .foreachBatch(write_to_cassandra)
        # Durable checkpoint on shared object storage so a restart resumes from
        # the last committed Kafka offsets instead of reprocessing / dropping data.
        .option("checkpointLocation", checkpoint_location(JOB_NAME))
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
