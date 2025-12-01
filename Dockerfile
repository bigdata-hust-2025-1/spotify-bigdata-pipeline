FROM apache/spark:3.5.3

USER root

WORKDIR /opt/spark/work-dir

COPY spark_jobs/ /opt/spark/work-dir/spark_jobs/

# --- THAY ĐỔI QUAN TRỌNG Ở ĐÂY ---
# 1. Tạo trước thư mục cache cho Ivy (thư viện)
# 2. Cấp quyền sở hữu toàn bộ thư mục /home/spark và /opt/spark/work-dir cho user 185
RUN mkdir -p /home/spark/.ivy2 && \
    chown -R 185:185 /home/spark && \
    chown -R 185:185 /opt/spark/work-dir

USER 185