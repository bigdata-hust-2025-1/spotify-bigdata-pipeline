# KỊCH BẢN & TÀI LIỆU CHUẨN BỊ PHỎNG VẤN VỊ TRÍ DATA ENGINEER 

## 1. Giới thiệu tổng quan về Dự án (Elevator Pitch)
**Dự án: Spotify Behavior Data Platform**
- *"Dự án của em là một nền tảng xử lý dữ liệu hành vi người dùng end-to-end, được thiết kế theo kiến trúc Lambda/Kappa (Lakehouse) có tính mở rộng cao và Cloud-ready (chuẩn bị sẵn cấu hình cho Azure/AWS). Em sử dụng Kubernetes (AKS) để quản lý hoàn toàn các dịch vụ. Nền tảng chia làm ba pipeline chính: **Batch ETL** xử lý khối lượng lớn (Spark), **Streaming/Real-time** xử lý độ trễ thấp (Spark Structured Streaming), và **Real-time Anomaly Detection** (Apache Flink)."*
- *"Storage layer sử dụng triết lý Data Lakehouse với MinIO đứng vai trò Object Storage và Apache Iceberg làm Table Format nhằm tối ưu khả năng Update/Delete và Time-travel. Tại Serving Layer, em kết hợp Elasticsearch cho phân tích Full-text, Trino để query SQL phân tán trên Datalake, và Cassandra làm NoSQL Database cho các ứng dụng tốc độ cao."*
- *"Em còn tích hợp MLOps cơ bản bằng MLflow, phục vụ việc tự động huấn luyện (train) mô hình Machine Learning phát hiện tương tác bất thường."*

---

## 2. Ánh xạ với JD / Tại sao bạn phù hợp với Tech Stack của JD?

| Yêu cầu của bài toán (JD) | Công nghệ / Giải pháp em đang dùng trong dự án |
| :--- | :--- |
| **Kubernetes Data Platform** | Hệ thống chạy 100% trên `minikube` / Azure K8S (`aks_cluster`). Có manifest và Helm chart đầy đủ cho Spark Operator, Kafka, MinIO, v.v. |
| **Spark & PySpark** | Dùng Spark cho Batch (ETL dữ liệu dơ ở Bronze sang chuẩn hóa Silver và tổng hợp ở Gold). |
| **Apache Flink, Java/Scala** | Sử dụng **Flink (code Java)** để bắt trực tiếp event logs từ Kafka: cảnh báo user spam skip/bot streaming với độ trễ Mili-giây (Stateful processing). |
| **Kafka** | Làm Message Broker trung tâm đảm bảo decouple giữa Producer API và Consumer (Spark/Flink). |
| **Iceberg / Datalake** | Sử dụng **Apache Iceberg** thay vì Parquet truyền thống ở lớp Silver/Gold ghi xuống MinIO. Giúp hệ thống có ACID transaction, sửa xóa log hành vi dễ dàng. |
| **SQL & NoSQL: Cassandra/Trino** | Sử dụng **Cassandra** làm hot-storage cho real-time metrics. **Trino** làm SQL Query Engine cho Data Scientist. (Có code Pipeline từ Stream -> Cassandra). |
| **Data Cloud (Azure)** | Có thư mục `azure_iac` dùng Terraform khởi tạo `Azure Kubernetes Service` (AKS) và `Azure Data Lake Storage Gen2` (ADLS Gen2). Thể hiện kỹ năng Cloud-Native tốt. |
| **Machine Learning / MLOps** | Viết script phân tích hành vi cô lập (Isolation Forest) theo dấu `MLflow`. Cung cấp model có sẵn cho hệ thống Inference. |

---

## 3. Các câu hỏi phỏng vấn có thể gặp và Cách trả lời (Q&A)

### Q1: Tại sao em lại cấu trúc dữ liệu theo Bronze, Silver, Gold? 
**Trả lời:** Kiến trúc Medallion này (xuất phát từ Databricks) đảm bảo luồng dữ liệu minh bạch và khôi phục dễ dàng.
- **Bronze (Raw):** Chứa dữ liệu gốc, chưa biến đổi. Nếu logic hệ thống sai, em luôn có thể chạy lại từ class này.
- **Silver (Cleaned):** Em dùng Spark loại bỏ Null, chuẩn hoá kiểu dữ liệu, lưu dưới dạng **Apache Iceberg**, giúp Data Scientist có môi trường "sạch" để train model.
- **Gold (Business Level):** Nơi chứa Aggregate tables (tính tổng, đếm số bài hát), dữ liệu chuẩn bị sẵn để báo cáo.

### Q2: Tại sao dự án của em sử dụng bảng Apache Iceberg mà không lưu Parquet trực tiếp?
**Trả lời:** Dữ liệu hành vi người dùng thường có sự thay đổi chậm (Late arriving data) hoặc yêu cầu bổ sung/xóa do quy định (như GDPR). 
- Parquet truyền thống là các file bất biến (Immutable), mỗi lần sửa phải overwrite cả phân vùng (partition).
- **Iceberg** quản lý metadata thông minh. Nó mang tính năng ACID (Atomicity, Consistency, Isolation, Durability) giống Database quan hệ lên Datalake, giúp em Update, Delete row level hoặc Roll-back time-travel nhanh chóng mà không phải chạy lại toàn bộ pipeline.

### Q3: Vì sao em dùng cả Spark và Flink? Khác biệt ở đây là gì?
**Trả lời:**
- **Spark / PySpark** được em ưu tiên cho **Batch ETL** với lượng dữ liệu tỷ records do khả năng map-reduce và throughput (lưu lượng) khổng lồ.
- Tuy nhiên, Spark Streaming thực chất là "Micro-batch" (chạy theo lô rất nhỏ). Đối với task bảo mật như **Anomaly Detection** (Ví dụ user spam skip track trong 1s), hệ thống cần tính true-streaming và độ trễ cực thấp. **Apache Flink** là sự lựa chọn ưu việt nhất vì nó thiết kế cho Native Streaming, xử lý từng event đơn lẻ hiệu quả hơn.

### Q4: MLOps / Machine Learning trong dự án Data Engineer của em đóng vai trò gì?
**Trả lời:** Là Data Engineer, nhiệm vụ của em không phải là ra sức tinh chỉnh độ chính xác của Model (đó là việc của Data Scientist). Nhiệm vụ của em ở file `train_anomaly_model.py` là xây dựng pipeline tự động hóa: Lấy dữ liệu từ Trino/Iceberg -> Gọi code training model cơ bản -> Đóng gói và lưu trữ metadata bằng **MLflow**. Điều này giúp Data Warehouse gắn kết trơn tru với ML Lifecycle.

### Q5: Em sử dụng Azure Terraform/IaC để làm gì?
**Trả lời:** Em sử dụng mã Infrastucture as Code (Terraform) giúp tự động tạo cụm Azure Kubernetes (AKS) và storage ADLS Gen2. Mô hình này giúp doanh nghiệp tái sử dụng, triển khai test env hoặc prod env trên cloud trong vài phút chỉ với 1 dòng lệnh thay vì click chuột bằng tay.

---

## 4. Góp ý thể hiện Phong thái khi Phỏng vấn
- Hãy mở Repo GitHub trực tiếp chia sẻ luồng Code của mình. 
- Mở thư mục `spark_jobs/batch` nói về việc dùng Spark tích hợp Iceberg config ra sao.
- Mở thư mục `flink_jobs` để review kinh nghiệm viết OOP Java với luồng Source -> Transformation -> Sink.
- Tự tin khẳng định bản thân thành thạo Docker/Kubernetes và khả năng tự nghiên cứu Cloud (Azure/MinIO) để thiết kế hệ thống lớn.