import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Cho phép import package `common` ở gốc repo khi chạy qua spark-submit.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.config import TOPIC_PLAYBACK  # noqa: E402

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = TOPIC_PLAYBACK  # Tên topic thống nhất từ common.config (finding A2)
MONGO_URI = "mongodb://mongodb.bigdata:27017/spotify_db.playback_events"

def main():
    spark = SparkSession.builder \
        .appName("Spotify_Mongo_Stream_Advanced") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Schema
    event_schema = StructType([
        StructField("track_id", StringType()),
        StructField("track_name", StringType()),
        StructField("timestamp", DoubleType()),
        StructField("event_time_str", StringType()),
        StructField("location", StringType()),
        StructField("device", StringType()),
        StructField("status", StringType())
    ])

    # Read Kafka
    print("--> Reading from Kafka...")
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse
    df_parsed = df_kafka.select(
        F.from_json(F.col("value").cast("string"), event_schema).alias("data")
    ).select("data.*")

    # --- [ADVANCED] Watermarking & Time Handling ---
    df_clean = df_parsed \
        .withColumn("event_ts", F.to_timestamp(F.col("timestamp"))) \
        .withColumn("processed_timestamp", F.current_timestamp()) \
        .withWatermark("event_ts", "10 minutes")
    # -----------------------------------------------

    print("--> Streaming to MongoDB...")
    query = df_clean.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "/tmp/checkpoint/spotify_mongo_stream") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .option("spark.mongodb.connection.uri", MONGO_URI) \
        .option("spark.mongodb.database", "spotify_db") \
        .option("spark.mongodb.collection", "playback_events") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()