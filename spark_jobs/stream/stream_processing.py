import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

# --- CẤU HÌNH ---
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
# Lưu ý: Topic này phải khớp với bên Producer
TOPIC = "spotify_playback_events"
MONGO_URI = "mongodb://mongodb.bigdata:27017/spotify_db.playback_events"

def main():
    spark = SparkSession.builder \
        .appName("Spotify_Event_Stream_Processor") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Định nghĩa Schema cho Sự Kiện Nghe Nhạc (User Playback Event)
    # Phải khớp 100% với JSON mà produce_to_kafka.py gửi lên
    event_schema = StructType([
        StructField("track_id", StringType()),
        StructField("track_name", StringType()),
        StructField("artist_id", StringType()),
        StructField("album_id", StringType()),
        StructField("track_popularity", IntegerType()),
        StructField("event_id", StringType()),
        StructField("user_id", StringType()),
        StructField("timestamp", DoubleType()),      # Epoch time
        StructField("event_time_str", StringType()), # String time
        StructField("location", StringType()),
        StructField("device", StringType()),
        StructField("listen_duration_ms", IntegerType()),
        StructField("status", StringType())          # skipped / completed
    ])

    # 2. Đọc Stream từ Kafka
    print(f"--> Listening to Kafka Topic: {TOPIC}...")
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 3. Parse JSON & Transform
    df_parsed = df_kafka.select(
        F.from_json(F.col("value").cast("string"), event_schema).alias("data")
    ).select("data.*")

    # Thêm các cột dẫn xuất để tiện Analytics sau này
    df_clean = df_parsed \
        .withColumn("is_completed", F.when(F.col("status") == "completed", 1).otherwise(0)) \
        .withColumn("is_skipped", F.when(F.col("status") == "skipped", 1).otherwise(0)) \
        .withColumn("processed_time", F.current_timestamp())

    # In schema ra log để debug (chỉ hiện lúc khởi động)
    print("--- Event Schema ---")
    df_clean.printSchema()

    # 4. Ghi vào MongoDB (Sink)
    print("--> Streaming to MongoDB...")
    query = df_clean.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "/tmp/checkpoint/spotify_events") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .option("spark.mongodb.connection.uri", MONGO_URI) \
        .option("spark.mongodb.database", "spotify_db") \
        .option("spark.mongodb.collection", "playback_events") \
        .outputMode("append") \
        .trigger(processingTime='5 seconds') \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()