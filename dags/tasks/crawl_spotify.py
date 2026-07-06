# ingestion/crawl_spotify.py
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from kafka import KafkaProducer
import json
import os
import time
from datetime import datetime
from typing import List, Dict, Set
from kafka.admin import KafkaAdminClient, NewTopic
# from dotenv import load_dotenv
# load_dotenv()

# === CẤU HÌNH ===
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_ALBUMS = os.getenv("KAFKA_TOPIC_ALBUMS", "spotify_albums")
KAFKA_TOPIC_TRACKS = os.getenv("KAFKA_TOPIC_TRACKS", "spotify_tracks")
KAFKA_TOPIC_ARTISTS = os.getenv("KAFKA_TOPIC_ARTISTS", "spotify_artists")
LIMIT_PER_PAGE = int(os.getenv("LIMIT_PER_PAGE", "50"))

# NOTE (PR-11): clients are built lazily inside the factories below — never at
# import time. Airflow parses this module every few seconds while scanning the
# DAGs folder; constructing a Spotify client / KafkaProducer / KafkaAdminClient
# (or, worse, running a full crawl) at module scope opened live network
# connections on every parse. The Airflow task calls run_crawl(), which owns the
# lifecycle of every client it creates.


def build_spotify():
    """Construct an authenticated Spotify client. Fails fast on missing creds."""
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        raise EnvironmentError(
            "SPOTIFY_CLIENT_ID và SPOTIFY_CLIENT_SECRET phải được thiết lập!"
        )
    return spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
    ))


def build_producer():
    """Construct the Kafka producer used to publish crawled items."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5,
        max_block_ms=300000,
        request_timeout_ms=30000,
        acks='all',
    )


# Hàm tạo topic nếu chưa tồn tại
def create_topic_if_not_exists(topic_name, bootstrap_servers, num_partitions=6, replication_factor=1):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    existing_topics = admin_client.list_topics()
    
    if topic_name not in existing_topics:
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        admin_client.create_topics([topic])
        print(f"Created topic: {topic_name}")
    else:
        print(f"Topic {topic_name} already exists.")
    
    admin_client.close()


# ==================== HÀM TỐI GIẢN DỮ LIỆU ====================
def simplify_album(album: Dict) -> Dict:
    return {
        "album_type": album.get("album_type"),
        "total_tracks": album.get("total_tracks"),
        "id": album.get("id"),
        "name": album.get("name"),
        "release_date": album.get("release_date"),
        "type": album.get("type"),
        "artists": [{"id": a.get("id")} for a in album.get("artists", [])],
        "copyrights": [
            {"text": c.get("text"), "type": c.get("type")}
            for c in album.get("copyrights", [])
        ],
        "genres": album.get("genres", []),
        "label": album.get("label"),
        "popularity": album.get("popularity"),
        "timestamp": datetime.now().isoformat()  # Thêm timestamp để nhất quán
    }

def simplify_track(track: Dict, album_id: str, album_release_date: str) -> Dict:
    return {
        "album": {"id": album_id},
        "artists": [{"id": a.get("id")} for a in track.get("artists", [])],
        "disc_number": track.get("disc_number"),
        "duration_ms": track.get("duration_ms"),
        "explicit": track.get("explicit"),
        "release_date": album_release_date,  # nhất quán từ album
        "id": track.get("id"),
        "is_local": track.get("is_local"),
        "name": track.get("name"),
        "popularity": track.get("popularity"),
        "track_number": track.get("track_number"),
        "type": track.get("type"),
        "timestamp": datetime.now().isoformat()  # Thêm timestamp
    }

def simplify_artist(artist: Dict) -> Dict:
    return {
        "followers": artist.get("followers"),
        "genres": artist.get("genres"),
        "id": artist.get("id"),
        "name": artist.get("name"),
        "popularity": artist.get("popularity"),
        "type": artist.get("type"),
        "timestamp": datetime.now().isoformat()  # Thêm timestamp
    }

# ==================== HÀM LẤY TOÀN BỘ ALBUMS MỚI (PAGINATION) ====================
def get_all_new_releases(sp, limit_per_page: int = 50) -> List[Dict]:
    all_albums_basic = []
    offset = 0
    while True:
        print(f"Đang lấy albums từ offset {offset}...")
        try:
            response = sp.new_releases(limit=limit_per_page, offset=offset)
            items = response['albums']['items']
            if not items:  # Không còn items → hết
                break
            all_albums_basic.extend(items)
            offset += len(items)  # Tăng offset chính xác dựa trên số items thực tế
            time.sleep(0.2)  # Tránh rate limit
        except spotipy.SpotifyException as e:
            print(f"[Spotify API Error] {e.http_status} - {e.msg}")
            if e.http_status == 429:  # Rate-limit
                retry_after = int(e.headers.get('Retry-After', 10))
                print(f"[Rate Limit] Tạm dừng {retry_after}s...")
                time.sleep(retry_after)
            break
        except Exception as e:
            print(f"[Lỗi] {e}")
            break
    print(f"Tổng albums lấy được: {len(all_albums_basic)} (total từ API: {response['albums']['total'] if 'response' in locals() else 'N/A'})")
    return all_albums_basic

# ==================== HÀM CRAWL DỮ LIỆU NEW RELEASES ====================
def crawl_new_releases(sp=None):
    if sp is None:
        sp = build_spotify()
    print("Đang lấy toàn bộ albums mới nhất (global)...")

    albums_basic = get_all_new_releases(sp, LIMIT_PER_PAGE)
    
    simplified_albums: List[Dict] = []
    simplified_tracks: List[Dict] = []
    all_artist_ids: Set[str] = set()
    
    for idx, basic_album in enumerate(albums_basic, 1):
        album_id = basic_album["id"]
        print(f"  [{idx}/{len(albums_basic)}] Xử lý album: {basic_album['name']} ({album_id})")
        
        try:
            # Lấy chi tiết album đầy đủ (có tracks, genres, label...)
            album = sp.album(album_id)
            release_date = album.get("release_date")
            
            # Tối giản album
            simplified_albums.append(simplify_album(album))
            
            # Thu thập artist_id từ album
            for artist in album.get("artists", []):
                all_artist_ids.add(artist["id"])
            
            # Xử lý tracks
            for track in album.get("tracks", {}).get("items", []):
                # Tối giản track
                simplified_tracks.append(simplify_track(track, album_id, release_date))
                
                # Thu thập artist_id từ track (bao gồm featured)
                for artist in track.get("artists", []):
                    all_artist_ids.add(artist["id"])
            
            # Nghỉ nhẹ để tránh rate limit
            time.sleep(0.2)
        
        except spotipy.SpotifyException as e:
            print(f"[Spotify API Error for album {album_id}] {e.http_status} - {e.msg}")
            if e.http_status == 429:
                retry_after = int(e.headers.get('Retry-After', 10))
                print(f"[Rate Limit] Tạm dừng {retry_after}s...")
                time.sleep(retry_after)
            continue
        except Exception as e:
            print(f"[Lỗi for album {album_id}] {e}")
            continue
    
    # ==================== LẤY CHI TIẾT ARTISTS (BATCH) ====================
    print(f"\nTìm thấy {len(all_artist_ids)} artist duy nhất. Đang lấy thông tin chi tiết...")
    artist_ids_list = list(all_artist_ids)
    simplified_artists: List[Dict] = []
    
    # Batch 50 ID/lần (giới hạn của Spotify)
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
                print(f"[Rate Limit] Tạm dừng {retry_after}s...")
                time.sleep(retry_after)
        except Exception as e:
            print(f"[Lỗi for artists batch] {e}")
    
    print(f"\nĐã lấy dữ liệu: Albums: {len(simplified_albums)}, Tracks: {len(simplified_tracks)}, Artists: {len(simplified_artists)}")
    return simplified_albums, simplified_tracks, simplified_artists

# ==================== AIRFLOW TASK ENTRYPOINT ====================
def run_crawl(logical_date=None, sp=None, producer=None):
    """Crawl Spotify new releases and publish them to Kafka.

    This is the callable invoked by the Airflow ``crawl_spotify`` task. It owns
    the lifecycle of the clients it creates: unless a client is injected (tests),
    it builds its own Spotify + Kafka producer and always closes the producer in
    a ``finally`` block — so a failed run never leaks a live connection. Nothing
    here runs at import time (PR-11).

    ``logical_date`` is the DAG run's ``{{ ds }}`` (YYYY-MM-DD); it is threaded
    through for logging/lineage rather than a hardcoded literal.
    """
    if sp is None:
        sp = build_spotify()
    owns_producer = producer is None
    if producer is None:
        producer = build_producer()

    try:
        for topic in (KAFKA_TOPIC_ALBUMS, KAFKA_TOPIC_TRACKS, KAFKA_TOPIC_ARTISTS):
            create_topic_if_not_exists(topic, KAFKA_BOOTSTRAP)

        simplified_albums, simplified_tracks, simplified_artists = crawl_new_releases(sp)

        sent = 0
        for topic, items in (
            (KAFKA_TOPIC_ALBUMS, simplified_albums),
            (KAFKA_TOPIC_TRACKS, simplified_tracks),
            (KAFKA_TOPIC_ARTISTS, simplified_artists),
        ):
            for item in items:
                producer.send(topic, item)
                sent += 1

        producer.flush()
        print(f"[crawl] logical_date={logical_date} -> sent {sent} items to Kafka.")
        return sent
    finally:
        if owns_producer:
            producer.close()  # Đảm bảo close producer dù success hay error