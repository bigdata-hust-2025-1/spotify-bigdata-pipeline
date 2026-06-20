from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Stream_to_Cassandra") \
    .config("spark.cassandra.connection.host", "cassandra-node") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Giả định đọc stream từ Kafka (chứa dữ liệu đã qua xử lý hoặc real-time metrics)
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "spotify-gold-events") \
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
