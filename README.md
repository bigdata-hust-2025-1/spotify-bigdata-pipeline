# spotify-bigdata-pipeline
Hệ thống thu thập, lưu trữ, phân tích dữ liệu âm nhạc từ Spotify API. Big Data Project - HUST 2025-1
minikube start
minikube kubectl -- port-forward svc/minio -n bigdata 9001:9001 # minio
minikube kubectl -- port-forward svc/minio -n bigdata 9000:9000
[http://localhost:9001/](http://localhost:9001/)
minikube kubectl -- port-forward svc/kafka-ui -n bigdata 8080:8080 #kafka
[http://localhost:8080/](http://localhost:8080/)
minikube kubectl -- port-forward svc/kafka -n bigdata 9092:9092 #kafka

docker run -it --rm `
    -v "${PWD}/data:/data" `
    -v "${PWD}/spark_jobs:/opt/spark/work-dir/spark_jobs" `
    --add-host=minio.bigdata:host-gateway `
    --add-host=host.docker.internal:host-gateway `
    -e MINIO_ENDPOINT="http://host.docker.internal:9000" `
    -e MINIO_ACCESS_KEY="minioadmin" `
    -e MINIO_SECRET_KEY="miniopass123" `
    apache/spark:3.5.3 `
    /opt/spark/bin/spark-submit `
    --master local[*] `
    --conf spark.jars.ivy=/tmp/.ivy2 `
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 `
    local:///opt/spark/work-dir/spark_jobs/batch/bronze_to_silver_all.py