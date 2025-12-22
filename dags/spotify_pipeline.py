# dags/spotify_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import hàm từ 2 script
from tasks.crawl_spotify import crawl_new_releases, producer, KAFKA_TOPIC_ALBUMS, KAFKA_TOPIC_TRACKS, KAFKA_TOPIC_ARTISTS
from tasks.kafka_to_minio import consume_and_upload, TOPICS

default_args = {
    'owner': 'your_name',
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spotify_pipeline',
    default_args=default_args,
    description='Crawl Spotify → Kafka → MinIO',
    schedule_interval='*/7 * * * *',  # Chạy hàng ngày (có thể đổi '@hourly' để test)
    start_date=datetime(2025, 12, 22),
    catchup=False,
) as dag:

    def crawl_task():
        """Task 1: Crawl và đẩy vào Kafka"""
        simplified_albums, simplified_tracks, simplified_artists = crawl_new_releases()

        sent = 0
        for item in simplified_albums:
            producer.send(KAFKA_TOPIC_ALBUMS, item)
            sent += 1
        for item in simplified_tracks:
            producer.send(KAFKA_TOPIC_TRACKS, item)
            sent += 1
        for item in simplified_artists:
            producer.send(KAFKA_TOPIC_ARTISTS, item)
            sent += 1

        producer.flush()
        print(f"Đã gửi {sent} items vào Kafka.")
        producer.close()  # Đóng producer sau khi xong

    def consume_task():
        """Task 2: Consume từ Kafka và đẩy lên MinIO"""
        print("Bắt đầu consume và upload lên MinIO...")
        for topic, folder in TOPICS.items():
            consume_and_upload(topic, folder)
        print("Tất cả topics đã xử lý xong!")

    crawl = PythonOperator(
        task_id='crawl_spotify',
        python_callable=crawl_task,
    )

    consume = PythonOperator(
        task_id='consume_to_minio',
        python_callable=consume_task,
    )

    crawl >> consume  # Thứ tự: crawl xong mới chạy consume