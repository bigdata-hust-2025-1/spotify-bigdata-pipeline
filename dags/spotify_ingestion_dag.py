# dags/spotify_ingestion_dag.py
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from kafka import KafkaProducer, KafkaConsumer
import json
import os
import time
from typing import List, Dict, Set
from minio import Minio
from minio.error import S3Error
import io

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spotify_ingestion_pipeline',
    default_args=default_args,
    description='Crawl Spotify → Kafka → MinIO Bronze liền mạch mỗi 5 phút',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2025, 12, 21),
    catchup=False,
    tags=['spotify', 'ingestion'],
) as dag:

    def crawl_and_send_to_kafka(**kwargs):
        # Logic producer từ crawl_spotify.py (batch mode)
        SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
        SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
        KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        KAFKA_TOPIC_ALBUMS = os.getenv("KAFKA_TOPIC_ALBUMS", "spotify_albums")
        KAFKA_TOPIC_TRACKS = os.getenv("KAFKA_TOPIC_TRACKS", "spotify_tracks")
        KAFKA_TOPIC_ARTISTS = os.getenv("KAFKA_TOPIC_ARTISTS", "spotify_artists")
        LIMIT_PER_PAGE = int(os.getenv("LIMIT_PER_PAGE", "50"))

        if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
            raise EnvironmentError("SPOTIFY_CLIENT_ID và SPOTIFY_CLIENT_SECRET phải được thiết lập!")

        sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET
        ))

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            acks='all'
        )

        try:
            def simplify_album(album: Dict) -> Dict:
                return {
                    "album_type": album.get("album_type"),
                    "total_tracks": album.get("total_tracks"),
                    "id": album.get("id"),
                    "name": album.get("name"),
                    "release_date": album.get("release_date"),
                    "type": album.get("type"),
                    "artists": [{"id": a.get("id")} for a in album.get("artists", [])],
                    "copyrights": [{"text": c.get("text"), "type": c.get("type")} for c in album.get("copyrights", [])],
                    "genres": album.get("genres", []),
                    "label": album.get("label"),
                    "popularity": album.get("popularity"),
                    "timestamp": datetime.now().isoformat()
                }

            def simplify_track(track: Dict, album_id: str, album_release_date: str) -> Dict:
                return {
                    "album": {"id": album_id},
                    "artists": [{"id": a.get("id")} for a in track.get("artists", [])],
                    "disc_number": track.get("disc_number"),
                    "duration_ms": track.get("duration_ms"),
                    "explicit": track.get("explicit"),
                    "release_date": album_release_date,
                    "id": track.get("id"),
                    "is_local": track.get("is_local"),
                    "name": track.get("name"),
                    "popularity": track.get("popularity"),
                    "track_number": track.get("track_number"),
                    "type": track.get("type"),
                    "timestamp": datetime.now().isoformat()
                }

            def simplify_artist(artist: Dict) -> Dict:
                return {
                    "followers": artist.get("followers"),
                    "genres": artist.get("genres"),
                    "id": artist.get("id"),
                    "name": artist.get("name"),
                    "popularity": artist.get("popularity"),
                    "type": artist.get("type"),
                    "timestamp": datetime.now().isoformat()
                }

            def get_all_new_releases(sp, limit_per_page: int = 50) -> List[Dict]:
                all_albums_basic = []
                offset = 0
                while True:
                    print(f"Đang lấy albums từ offset {offset}...")
                    try:
                        response = sp.new_releases(limit=limit_per_page, offset=offset)
                        items = response['albums']['items']
                        if not items:
                            break
                        all_albums_basic.extend(items)
                        offset += len(items)
                        time.sleep(0.2)
                    except spotipy.SpotifyException as e:
                        print(f"[Spotify API Error] {e.http_status} - {e.msg}")
                        if e.http_status == 429:
                            retry_after = int(e.headers.get('Retry-After', 10))
                            print(f"[Rate Limit] Tạm dừng {retry_after}s...")
                            time.sleep(retry_after)
                        break
                    except Exception as e:
                        print(f"[Lỗi] {e}")
                        break
                print(f"Tổng albums lấy được: {len(all_albums_basic)}")
                return all_albums_basic

            def crawl_new_releases():
                print("Đang lấy toàn bộ albums mới nhất (global)...")
               
                albums_basic = get_all_new_releases(sp, LIMIT_PER_PAGE)
               
                simplified_albums = []
                simplified_tracks = []
                all_artist_ids = set()
               
                for idx, basic_album in enumerate(albums_basic, 1):
                    album_id = basic_album["id"]
                    print(f"  [{idx}/{len(albums_basic)}] Xử lý album: {basic_album['name']} ({album_id})")
                   
                    try:
                        album = sp.album(album_id)
                        release_date = album.get("release_date")
                       
                        simplified_albums.append(simplify_album(album))
                       
                        for artist in album.get("artists", []):
                            all_artist_ids.add(artist["id"])
                       
                        for track in album.get("tracks", {}).get("items", []):
                            simplified_tracks.append(simplify_track(track, album_id, release_date))
                           
                            for artist in track.get("artists", []):
                                all_artist_ids.add(artist["id"])
                       
                        time.sleep(0.2)
                   
                    except spotipy.SpotifyException as e:
                        print(f"[Spotify API Error for album {album_id}] {e.http_status} - {e.msg}")
                        if e.http_status == 429:
                            retry_after = int(e.headers.get('Retry-After', 10))
                            time.sleep(retry_after)
                        continue
                    except Exception as e:
                        print(f"[Lỗi for album {album_id}] {e}")
                        continue
               
                print(f"\nTìm thấy {len(all_artist_ids)} artist duy nhất. Đang lấy thông tin chi tiết...")
                artist_ids_list = list(all_artist_ids)
                simplified_artists = []
               
                for i in range(0, len(artist_ids_list), 50):
                    batch = artist_ids_list[i:i+50]
                    try:
                        results = sp.artists(batch)
                        for artist in results["artists"]:
                            simplified_artists.append(simplify_artist(artist))
                        time.sleep(0.3)
                    except spotipy.SpotifyException as e:
                        print(f"[Spotify API Error for artists batch] {e.http_status} - {e.msg}")
                        if e.http_status == 429:
                            retry_after = int(e.headers.get('Retry-After', 10))
                            time.sleep(retry_after)
                        continue
                    except Exception as e:
                        print(f"[Lỗi for artists batch] {e}")
               
                return simplified_albums, simplified_tracks, simplified_artists

            simplified_albums, simplified_tracks, simplified_artists = crawl_new_releases()
            sent = 0
            for item in simplified_albums:
                producer.send(KAFKA_TOPIC_ALBUMS, item)
                print(f"Sent album: {item.get('name', 'N/A')}")
                sent += 1
            for item in simplified_tracks:
                producer.send(KAFKA_TOPIC_TRACKS, item)
                print(f"Sent track: {item.get('name', 'N/A')}")
                sent += 1
            for item in simplified_artists:
                producer.send(KAFKA_TOPIC_ARTISTS, item)
                print(f"Sent artist: {item.get('name', 'N/A')}")
                sent += 1
            producer.flush()
            print(f"Đã gửi {sent} items vào Kafka.")
        finally:
            producer.close()

    def consume_and_save_to_bronze(**kwargs):
        # Logic consumer từ kafka_to_bronze.py (batch mode)
        KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        TOPICS = ["spotify_albums", "spotify_tracks", "spotify_artists"]
        GROUP_ID = "bronze-consumer-group-v1"
        MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
        MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "miniopass123")
        BRONZE_BUCKET = "spotify-bronze"

        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=GROUP_ID,
            auto_offset_reset='latest',  # Chỉ đọc data mới từ lần run này
            enable_auto_commit=False,
        )

        minio_client = Minio(MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET, secure=False)

        if not minio_client.bucket_exists(BRONZE_BUCKET):
            minio_client.make_bucket(BRONZE_BUCKET)
            print(f"Created bucket {BRONZE_BUCKET}")

        ingest_date = date.today().isoformat()
        buffers = {topic: [] for topic in TOPICS}

        print("[DEBUG] Polling Kafka for data...")
        msg_pack = consumer.poll(timeout_ms=10000)  # Tăng timeout để chắc chắn hơn

        print(f"[DEBUG] Poll result: {len(msg_pack)} topics with messages")

        for tp, messages in msg_pack.items():
            topic = tp.topic
            for message in messages:
                data = json.loads(message.value.decode('utf-8'))  # Sửa decode
                buffers[topic].append(json.dumps(data) + "\n")

        for topic in TOPICS:
            if buffers[topic]:
                domain = topic.replace("spotify_", "")
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                object_name = f"{domain}/ingest_date={ingest_date}/{timestamp}.jsonl"
                data_bytes = "".join(buffers[topic]).encode('utf-8')
                minio_client.put_object(BRONZE_BUCKET, object_name, io.BytesIO(data_bytes), len(data_bytes))
                print(f"Flushed {len(buffers[topic])} records for {topic}")

        consumer.commit()
        consumer.close()

    task_crawl = PythonOperator(
        task_id='crawl_spotify_to_kafka',
        python_callable=crawl_and_send_to_kafka,
    )

    task_bronze = PythonOperator(
        task_id='consume_kafka_to_minio_bronze',
        python_callable=consume_and_save_to_bronze,
    )

    task_crawl >> task_bronze