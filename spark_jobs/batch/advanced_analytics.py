import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.config import require_env  # noqa: E402

# --- CẤU HÌNH ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio.bigdata:9000")
ACCESS_KEY = require_env("MINIO_ACCESS_KEY")
SECRET_KEY = require_env("MINIO_SECRET_KEY")

# Ngày dữ liệu (Sửa lại cho đúng ngày bạn đã ingest)
INGEST_DATE = "2025-12-21"  

SILVER_PATH = f"s3a://spotify-silver/tracks/ingest_date={INGEST_DATE}"
GOLD_PATH = f"s3a://spotify-gold/ml_clustering/ingest_date={INGEST_DATE}"

def main():
    # 1. Khởi tạo Spark
    spark = SparkSession.builder \
        .appName("Spotify_KMeans_Clustering") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    print(f"--> Reading Data from Silver: {SILVER_PATH}")
    
    try:
        df = spark.read.parquet(SILVER_PATH)
        
        # 2. Chọn đặc trưng để phân cụm (Feature Engineering)
        # Phân cụm dựa trên: Thời lượng (duration_sec) và Độ phổ biến (popularity)
        feature_cols = ["duration_sec", "popularity"]
        
        # Lọc bỏ giá trị null và chọn cột cần thiết
        df_clean = df.select("track_id", "name", "duration_sec", "popularity") \
                     .dropna(subset=feature_cols)

        print("--> Preparing Features for MLlib...")
        
        # Gom các cột feature thành 1 cột vector
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df_vec = assembler.transform(df_clean)

        # Chuẩn hóa dữ liệu (StandardScaler) để dữ liệu đồng nhất
        scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures",
                                withStd=True, withMean=False)
        scalerModel = scaler.fit(df_vec)
        df_scaled = scalerModel.transform(df_vec)

        # 3. Training K-Means Model
        # Chia thành 3 nhóm (k=3)
        print("--> Training K-Means Model (k=3)...")
        kmeans = KMeans(featuresCol="scaledFeatures", k=3, seed=1)
        model = kmeans.fit(df_scaled)

        # 4. Dự đoán (Predictions)
        predictions = model.transform(df_scaled)

        # Đổi tên cột prediction thành 'cluster_id'
        results = predictions.select("track_id", "name", "duration_sec", "popularity", "prediction") \
                             .withColumnRenamed("prediction", "cluster_id")

        # 5. Lưu kết quả vào Gold Layer
        print(f"--> Writing ML Results to Gold: {GOLD_PATH}")
        results.write.mode("overwrite").parquet(GOLD_PATH)
        
        # In tâm cụm (Cluster Centers) ra màn hình để xem
        centers = model.clusterCenters()
        print("Cluster Centers (Scaled): ")
        for center in centers:
            print(center)

        print("✅ ML Job Completed Successfully!")

    except Exception as e:
        print(f"❌ ML Job Failed: {e}")

    spark.stop()

if __name__ == "__main__":
    main()