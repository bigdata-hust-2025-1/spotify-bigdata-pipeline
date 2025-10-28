# ingestion/crawl_spotify.py
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from kafka import KafkaProducer
import json
import os
import time
from datetime import datetime

# === CẤU HÌNH ===
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "spotify_tracks")
PLAYLIST_ID = "5Ip7qLwuvGAluWE2PBrBNl"
MAX_TRACKS = 50                                 # Chỉ lấy 50 bài

if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
    raise EnvironmentError("SPOTIFY_CLIENT_ID và SPOTIFY_CLIENT_SECRET phải được thiết lập!")

# === KHỞI TẠO SPOTIFY ===
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=SPOTIFY_CLIENT_ID,
    client_secret=SPOTIFY_CLIENT_SECRET
))

# === KHỞI TẠO KAFKA PRODUCER ===
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=3,
    acks='all'
)
print(f"Connected to Kafka.")

def get_first_n_tracks():
    """
    Lấy **đúng MAX_TRACKS (50) bài hát đầu tiên** của playlist.
    """
    tracks = []
    try:
        print(f"[Spotify] Lấy {MAX_TRACKS} bài đầu tiên từ playlist {PLAYLIST_ID}...")

        # Dùng playlist_items + limit = MAX_TRACKS → Spotify tự trả về tối đa 50
        results = sp.playlist_items(
            playlist_id=PLAYLIST_ID,
            fields="items(track(id,name,artists(name),album(name,release_date),duration_ms,popularity)),total",
            limit=MAX_TRACKS,
            offset=0
        )

        total_available = results.get('total', 0)
        print(f"[Spotify] Playlist có {total_available} bài, đang lấy {min(MAX_TRACKS, total_available)} bài.")

        for item in results['items']:
            t = item.get('track')
            if t and t.get('id'):
                tracks.append({
                    "id": t['id'],
                    "name": t['name'],
                    "artist": t['artists'][0]['name'],
                    "album": t['album']['name'],
                    "release_date": t['album']['release_date'],
                    "duration_ms": t['duration_ms'],
                    "popularity": t['popularity'],
                    "timestamp": datetime.now().isoformat()
                })

        print(f"[Spotify] Đã lấy thành công {len(tracks)} bài.")
        return tracks

    except spotipy.SpotifyException as e:
        print(f"[Spotify API Error] {e.http_status} - {e.msg}")
        if e.http_status == 429:                     # Rate-limit
            retry_after = int(e.headers.get('Retry-After', 10))
            print(f"[Rate Limit] Tạm dừng {retry_after}s...")
            time.sleep(retry_after)
        return []
    except Exception as e:
        print(f"[Lỗi] {e}")
        return []


# === VÒNG LẶP CHÍNH ===
print("Spotify → Kafka Producer started...")
print(f"   Playlist : {PLAYLIST_ID}")
print(f"   Topic    : {KAFKA_TOPIC}")
print(f"   Max tracks per run: {MAX_TRACKS}")
print(f"   Interval : 60 giây\n")

while True:
    try:
        tracks = get_first_n_tracks()

        if not tracks:
            print("[Warning] Không có dữ liệu để gửi. Thử lại sau 60s...")
        else:
            sent = 0
            for track in tracks:
                producer.send(KAFKA_TOPIC, track)
                print(f"Sent: {track['name']} - {track['artist']}")
                sent += 1
            producer.flush()
            print(f"Đã gửi {sent} bài vào Kafka.\n")

        time.sleep(60)

    except KeyboardInterrupt:
        print("\nDừng bởi người dùng. Đang thoát...")
        producer.close()
        break
    except Exception as e:
        print(f"[Lỗi nghiêm trọng] {e}")
        time.sleep(10)