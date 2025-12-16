import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Cấu hình Kafka & Elasticsearch
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "spotify_playback_events"
# Cấu hình ES
ES_NODES = "elasticsearch.bigdata"
ES_PORT = "9200"
ES_RESOURCE = "realtime_events/doc" # Index/Type

def main():
    spark = SparkSession.builder.appName("Spotify_ELK_Stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Schema (Giữ nguyên)
    event_schema = StructType([
        StructField("track_id", StringType()),
        StructField("track_name", StringType()),
        StructField("artist_id", StringType()),
        StructField("album_id", StringType()),
        StructField("track_popularity", IntegerType()),
        StructField("event_id", StringType()),
        StructField("user_id", StringType()),
        StructField("timestamp", DoubleType()),
        StructField("event_time_str", StringType()),
        StructField("location", StringType()),
        StructField("device", StringType()),
        StructField("listen_duration_ms", IntegerType()),
        StructField("status", StringType())
    ])

    # 2. Read Kafka
    df_kafka = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest").load()

    # 3. Transform
    df_parsed = df_kafka.select(F.from_json(F.col("value").cast("string"), event_schema).alias("data")).select("data.*")
    
    df_clean = df_parsed \
        .withColumn("processed_timestamp", F.current_timestamp()) \
        .withColumn("is_completed", F.when(F.col("status") == "completed", 1).otherwise(0))

    # 4. Write to Elasticsearch
    print("--> Streaming to Elasticsearch...")
    query = df_clean.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .option("checkpointLocation", "/tmp/checkpoint/spotify_es_stream") \
        .option("es.nodes", ES_NODES) \
        .option("es.port", ES_PORT) \
        .option("es.resource", ES_RESOURCE) \
        .option("es.nodes.wan.only", "true") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()