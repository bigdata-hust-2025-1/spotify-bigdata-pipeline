# 🎵 Spotify Behavior Data Platform & ML-Ops Hybrid Architecture

![Kubernetes](https://img.shields.io/badge/kubernetes-%23326ce5.svg?style=for-the-badge&logo=kubernetes&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black)
![Apache Flink](https://img.shields.io/badge/Apache_Flink-E6526F?style=for-the-badge&logo=apache-flink&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Cassandra](https://img.shields.io/badge/Cassandra-1287B1?style=for-the-badge&logo=apache-cassandra&logoColor=white)
![Azure](https://img.shields.io/badge/azure-%230072C6.svg?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Iceberg](https://img.shields.io/badge/Apache_Iceberg-45B0A8?style=for-the-badge&logoColor=white)

---

## 📖 Giới thiệu

Dự án Big Data xử lý dữ liệu hành vi âm nhạc Spotify theo kiến trúc **Lakehouse** và **Streaming Real-time** hiện đại. 
Dự án được triển khai trên nền tảng Cloud-native Kubernetes Platform, tích hợp ML-Ops & Data Cloud phù hợp với các tiêu chuẩn quy mô Enterprise.

---

## 🎯 Điểm nhấn Kỹ thuật (Key Features)

* **Kubernetes Data Platform**: Triển khai ecosystem (Spark, Kafka, MinIO/Azure DL) trên Kubernetes (AKS).
* **Lakehouse Architecture**: Xây dựng kiến trúc Medallion (Bronze - Silver - Gold) với **Apache Iceberg**, đảm bảo tính toàn vẹn (ACID Transactions).
* **High-Throughput ETL (Batch)**: Apache Spark + PySpark thực hiện làm sạch dữ liệu lớn hằng ngày. 
* **Ultra Low-Latency Streaming**: Tích hợp luồng event (nghe nhạc, skip bài) với **Kafka** và **Apache Flink (Java)** nhằm bắt các tương tác bất thường realtime.
* **Serving Database Mix**: Cung cấp **Trino** làm SQL Query Engine và **Cassandra** cho NoSQL Dashboard response <10ms.
* **MLOps**: Kết hợp thư viện **MLflow** vào quá trình training Pipeline để huấn luyện mô hình học máy (Isolation Forest) phát hiện dị thường.
* **Infrastructure as Code (Azure)**: Quản lý hạ tầng đám mây Azure thông qua Terraform (`azure_iac`). 

---

## 🏗️ Kiến trúc hệ thống (High-Level Architecture)

```mermaid
graph TD
    subgraph SOURCES [Ingestion Layer]
        App[Spotify App / Web] -->|Streaming Events| Kafka[Apache Kafka]
        File[Historical Logs] -->|Batch Upload| MinIO[(Data Lake - MinIO/ADLS)]
    end

    subgraph STREAMING [Hot Path - Streaming]
        Kafka --> Flink[Apache Flink - Java]
        Flink -->|Anomaly Detected| Alert[Alert System]
        Kafka --> SparkStream[Spark Streaming]
        SparkStream --> Cassandra[(NoSQL: Cassandra)]
    end

    subgraph BATCH [Cold Path - Lakehouse]
        MinIO --> Bronze[Bronze Layer: Raw]
        Bronze -->|Spark ETL| Silver[Silver Layer: Iceberg]
        Silver -->|Spark Aggregate| Gold[Gold Layer: Iceberg/ES]
    end

    subgraph MLOPS [MLOps / Lifecycle]
        Silver -->|Training Data| ML[MLFlow & Sklearn]
        ML -->|Isolation Forest| Model[ML Model Registry]
    end

    subgraph SERVING [Serving & Analytics]
        Cassandra --> Dashboard[Realtime Dashboard]
        Gold --> Trino[Trino SQL Engine]
    end
```

---

## 📁 Cấu trúc Thư mục

* `/spark_jobs`: Code ETL Batch/Streaming với PySpark và Iceberg.
* `/flink_jobs`: Project Java Flink handle Realtime Anomaly Detection.
* `/kubernetes`: K8s Manifests hạ tầng.
* `/mlops`: Pipeline machine learning train model.
* `/azure_iac`: Terraform script chạy Azure Cloud.
* `/docs`: Tài liệu chuẩn bị phỏng vấn, kiến trúc và concept.

*(📝 Xem giải thích kỹ thuật và các quyết định kiến trúc tại `/docs/INTERVIEW_DOCUMENTATION.md`)*

---

## ⚙️ Cấu hình (Environment Variables)

Các job đọc cấu hình từ biến môi trường để không phụ thuộc vào máy cụ thể. Các biến sau được thêm/chuẩn hoá gần đây:

| Biến | Mặc định | Dùng bởi | Mô tả |
| :--- | :--- | :--- | :--- |
| `INGEST_DATE` | `2025-12-21` | `spark_jobs/batch/bronze_to_silver_all.py`, `silver_to_gold_all.py` | Ngày partition dữ liệu cần xử lý. Airflow có thể truyền `{{ ds }}`. |
| `TRACKS_DATA_PATH` | `<repo>/data/tracks.json` | `spark_jobs/stream/produce_to_kafka.py` | Đường dẫn file track cho producer (mặc định trỏ vào repo, portable). |

*(Chi tiết thay đổi theo từng PR: xem `/docs/CHANGELOG.md`.)*
