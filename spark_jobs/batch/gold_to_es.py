import os
from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio.bigdata:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "miniopass123")
ES_NODES = "elasticsearch.bigdata"
ES_PORT = "9200"
INGEST_DATE = "2025-12-06" # Cập nhật ngày đúng

def main():
    spark = SparkSession.builder.appName("GoldToES") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    print(f"--> Syncing Batch Data to ES...")

    # Sync Artists Stats
    try:
        df = spark.read.parquet(f"s3a://spotify-gold/artists_stats/ingest_date={INGEST_DATE}")
        df.write.format("org.elasticsearch.spark.sql") \
            .option("es.nodes", ES_NODES).option("es.port", ES_PORT) \
            .option("es.resource", "batch_artists/doc") \
            .option("es.nodes.wan.only", "true").mode("overwrite").save()
        print("✅ Artists synced to ES.")
    except Exception as e: print(f"❌ Error: {e}")

    spark.stop()

if __name__ == "__main__":
    main()