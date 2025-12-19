# ingestion/crawl_spotify.py
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from kafka import KafkaProducer
import json
import os
import time
from datetime import datetime
from typing import List, Dict, Set

# === CẤU HÌNH ===
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_ALBUMS = os.getenv("KAFKA_TOPIC_ALBUMS", "spotify_albums")
KAFKA_TOPIC_TRACKS = os.getenv("KAFKA_TOPIC_TRACKS", "spotify_tracks")
KAFKA_TOPIC_ARTISTS = os.getenv("KAFKA_TOPIC_ARTISTS", "spotify_artists")
LIMIT_PER_PAGE = int(os.getenv("LIMIT_PER_PAGE", "50"))

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
def crawl_new_releases():
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

# === VÒNG LẶP CHÍNH ===
print("Spotify New Releases → Kafka Producer started...")
print(f"   Topics: Albums: {KAFKA_TOPIC_ALBUMS}, Tracks: {KAFKA_TOPIC_TRACKS}, Artists: {KAFKA_TOPIC_ARTISTS}")
print(f"   Limit per page: {LIMIT_PER_PAGE}")
print(f"   Interval : 60 giây\n")

while True:
    try:
        simplified_albums, simplified_tracks, simplified_artists = crawl_new_releases()

        sent = 0

        # Gửi albums
        for item in simplified_albums:
            producer.send(KAFKA_TOPIC_ALBUMS, item)
            item_type = item.get('type', 'unknown')
            item_name = item.get('name', 'N/A')
            print(f"Sent to {KAFKA_TOPIC_ALBUMS}: {item_type} - {item_name}")
            sent += 1

        # Gửi tracks
        for item in simplified_tracks:
            producer.send(KAFKA_TOPIC_TRACKS, item)
            item_type = item.get('type', 'unknown')
            item_name = item.get('name', 'N/A')
            print(f"Sent to {KAFKA_TOPIC_TRACKS}: {item_type} - {item_name}")
            sent += 1

        # Gửi artists
        for item in simplified_artists:
            producer.send(KAFKA_TOPIC_ARTISTS, item)
            item_type = item.get('type', 'unknown')
            item_name = item.get('name', 'N/A')
            print(f"Sent to {KAFKA_TOPIC_ARTISTS}: {item_type} - {item_name}")
            sent += 1

        producer.flush()
        print(f"Đã gửi {sent} items vào Kafka.\n")

        time.sleep(60)

    except KeyboardInterrupt:
        print("\nDừng bởi người dùng. Đang thoát...")
        producer.close()
        break
    except Exception as e:
        print(f"[Lỗi nghiêm trọng] {e}")
        time.sleep(10) 