import os
import sys
from pyspark.sql import SparkSession

# Cho phép import package `common` ở gốc repo khi chạy qua spark-submit.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.config import TOPIC_PLAYBACK  # noqa: E402

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra.bigdata")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka.bigdata:9092")

spark = SparkSession.builder \
    .appName("Stream_to_Cassandra") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Giả định đọc stream từ Kafka (chứa dữ liệu đã qua xử lý hoặc real-time metrics)
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC_PLAYBACK) \
    .load()

def write_to_cassandra(df, epoch_id):
    # Ghi từng batch của stream vào Cassandra
    df.write \
      .format("org.apache.spark.sql.cassandra") \
      .mode("append") \
      .options(table="user_plays", keyspace="spotify_ks") \
      .save()

# Viết dữ liệu vào Cassandra dưới chế độ stream
query = streaming_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("update") \
    .start()

query.awaitTermination()
