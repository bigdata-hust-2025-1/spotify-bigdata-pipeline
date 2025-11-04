import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json
import time
import os


# === Thiết lập client ===
client_id = "3377de6049224c2d920fe255ce00094b"
client_secret = "a90b9480131e41dc987ed30c21219c71"

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=client_id,
    client_secret=client_secret
))


# === Đọc file flattened albums ===
input_file = "all_albums_flattened.json"
output_file = "all_tracks_by_album.json"

with open(input_file, "r", encoding="utf-8") as f:
    all_albums = json.load(f)

print(f"Đã tải {len(all_albums)} albums từ file")

# Lấy 1000 mẫu đầu tiên
albums_to_process = all_albums[:1000]
print(f"Sẽ xử lý {len(albums_to_process)} albums đầu tiên")


# === Set toàn cục để đảm bảo track là duy nhất ===
global_seen_track_ids = set()


# === Đọc file tracks cũ (nếu có) ===
all_tracks_data = {}

if os.path.exists(output_file):
    with open(output_file, "r", encoding="utf-8") as f:
        try:
            all_tracks_data = json.load(f)
            # Nạp lại track IDs đã có
            for album_tracks in all_tracks_data.values():
                for track in album_tracks:
                    global_seen_track_ids.add(track['id'])
            print(f"Đã tải {len(global_seen_track_ids)} tracks từ file cũ")
            print(f"Đã tải {len(all_tracks_data)} albums từ file cũ")
        except Exception as e:
            print(f"Lỗi khi đọc file cũ: {e}")
            all_tracks_data = {}
else:
    all_tracks_data = {}


# === Lấy tracks theo từng album ===
total_albums = len(albums_to_process)
processed_count = 0
skipped_count = 0


for idx, album in enumerate(albums_to_process, start=1):
    album_id = album['id']
    album_name = album['name']
    
    # Bỏ qua album đã xử lý
    if album_id in all_tracks_data:
        print(f"[{idx}/{total_albums}] ⏭️  Bỏ qua '{album_name}' (đã xử lý)")
        skipped_count += 1
        continue
    
    print(f"[{idx}/{total_albums}] === Album: {album_name} ===")
    
    album_tracks = []
    
    # Lấy tracks bằng phân trang (mỗi lần 50 tracks, max 1000)
    limit_tracks = 50
    for offset in range(0, 1000, limit_tracks):
        try:
            # Lấy tracks của album
            results = sp.album_tracks(
                album_id=album_id,
                limit=limit_tracks,
                offset=offset
            )
            
            # Kiểm tra xem có dữ liệu không
            if 'items' not in results:
                print(f"  ⚠️  Không có items trong response")
                break
            
            tracks = results['items']
            if not tracks:
                break
            
            for track in tracks:
                # Kiểm tra track có đầy đủ thông tin không
                if 'id' not in track or 'name' not in track:
                    print(f"  ⚠️  Track thiếu thông tin, bỏ qua")
                    continue
                
                # Kiểm tra track ID có trùng không
                if track['id'] not in global_seen_track_ids:
                    album_tracks.append(track)
                    global_seen_track_ids.add(track['id'])
                    duration_min = track.get('duration_ms', 0) / 1000 / 60
                    print(f"  ✓ {track['name']} ({duration_min:.2f} min)")
                    time.sleep(0.05)
                else:
                    print(f"  ⊘ {track['name']} (trùng - bỏ qua)")
            
            # Kiểm tra có trang tiếp theo không
            if not results.get('next'):
                break
                
        except spotipy.exceptions.SpotifyException as e:
            print(f"  ⚠️  Lỗi Spotify API: {e}")
            if e.http_status == 429:  # Rate limit
                print(f"  ⏸️  Chờ 60 giây do rate limit...")
                time.sleep(60)
                continue
            else:
                time.sleep(1)
                break
        except Exception as e:
            print(f"  ⚠️  Lỗi không xác định: {e}")
            time.sleep(1)
            break
    
    # Lưu tracks của album
    all_tracks_data[album_id] = album_tracks
    processed_count += 1
    print(f"Đã lấy {len(album_tracks)} tracks mới cho '{album_name}'")
    
    # === GHI NGAY VÀO FILE JSON SAU MỖI ALBUM ===
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_tracks_data, f, ensure_ascii=False, indent=2)
        print(f"✅ Đã ghi album '{album_name}' vào {output_file}\n")
    except Exception as e:
        print(f"⚠️  Lỗi khi ghi file: {e}\n")


# Tổng tracks (không trùng lặp)
total_tracks = len(global_seen_track_ids)
print(f"\n{'='*60}")
print(f"🎉 Hoàn tất!")
print(f"   • Tổng albums cần xử lý: {total_albums}")
print(f"   • Đã xử lý: {processed_count}")
print(f"   • Bỏ qua: {skipped_count}")
print(f"   • Tổng tracks duy nhất: {total_tracks}")
print(f"   • Dữ liệu đã lưu vào: {output_file}")
print(f"{'='*60}")
