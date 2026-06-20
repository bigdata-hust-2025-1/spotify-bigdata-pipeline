import mlflow
import mlflow.sklearn
from sklearn.ensemble import IsolationForest
import pandas as pd

# Kết nối với MLflow Tracking Server (trong hệ sinh thái MLOps)
mlflow.set_tracking_uri("http://mlflow-server:5000")

def train_model(data_path):
    # Khởi tạo một phiên theo dõi trải nghiệm huấn luyện mô hình
    with mlflow.start_run():
        print(f"Loading data from {data_path}...")
        # Lấy dữ liệu từ Trino hoặc DataLake (dưới định dạng Parquet/Iceberg)
        df_train = pd.read_parquet(data_path)
        
        # Huấn luyện mô hình xác định bất thường Isolation Forest
        model = IsolationForest(contamination=0.01)
        # Giả sử chúng ta sử dụng 2 đặc điểm là thời lượng bài hát và số lần skip
        model.fit(df_train[['play_duration', 'skip_count']])
        
        # Ghi log lại mô hình vào MLflow để dễ dàng quản lý phiên bản và server sau này (Model Registry)
        mlflow.sklearn.log_model(model, "anomaly_isolation_forest")
        
        print("Model trained and logged to MLflow successfully!")

if __name__ == "__main__":
    train_model("s3a://datalake/gold/user_behaviors.parquet")
