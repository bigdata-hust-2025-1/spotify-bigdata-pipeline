# Sử dụng image chính thức Apache Spark 3.5.3
FROM apache/spark:3.5.3

# Chuyển sang root để cài đặt
USER root

# Thư mục làm việc chuẩn
WORKDIR /opt/spark/work-dir

# Copy code của bạn vào image
COPY spark_jobs/ /opt/spark/work-dir/spark_jobs/
#COPY src/ /opt/spark/work-dir/src/

# --- QUAN TRỌNG: Cấp quyền ghi cho user Spark (UID 185) ---
# Tạo thư mục cache ivy và cấp quyền sở hữu để tránh lỗi Permission Denied
RUN mkdir -p /home/spark/.ivy2 && \
    mkdir -p /tmp/checkpoint && \
    chown -R 185:185 /home/spark && \
    chown -R 185:185 /opt/spark/work-dir && \
    chown -R 185:185 /tmp/checkpoint

# Chuyển về user thường để chạy bảo mật
USER 185