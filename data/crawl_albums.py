import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json
import time
import os


# === Thiết lập client ===
client_id = "b4cd21310aa0411683fd4672cdafe001"
client_secret = "a413701f69444704ac8bfad66300a659"

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=client_id,
    client_secret=client_secret
))


# === Đọc file flattened artists ===
input_file = "all_artists_flattened.json"
output_file = "all_albums_by_artist.json"

with open(input_file, "r", encoding="utf-8") as f:
    all_artists = json.load(f)

print(f"Đã tải {len(all_artists)} artist từ file")

# Lấy 2000 mẫu đầu tiên
artists_to_process = all_artists[:2000]
print(f"Sẽ xử lý {len(artists_to_process)} artist đầu tiên")


# === Set toàn cục để đảm bảo album là duy nhất ===
global_seen_album_ids = set()

# === Đọc file albums cũ (nếu có) ===
all_albums_data = {}

if os.path.exists(output_file):
    with open(output_file, "r", encoding="utf-8") as f:
        try:
            all_albums_data = json.load(f)
            # Nạp lại album IDs đã có
            for artist_albums in all_albums_data.values():
                for album in artist_albums:
                    global_seen_album_ids.add(album['id'])
            print(f"Đã tải {len(global_seen_album_ids)} albums từ file cũ")
            print(f"Đã tải {len(all_albums_data)} artists từ file cũ")
        except Exception as e:
            print(f"Lỗi khi đọc file cũ: {e}")
            all_albums_data = {}
else:
    all_albums_data = {}


# === Lấy albums theo từng artist ===
total_artists = len(artists_to_process)
processed_count = 0
skipped_count = 0

for idx, artist in enumerate(artists_to_process, start=1):
    artist_id = artist['id']
    artist_name = artist['name']
    
    # Bỏ qua artist đã xử lý
    if artist_id in all_albums_data:
        print(f"[{idx}/{total_artists}] ⏭️  Bỏ qua '{artist_name}' (đã xử lý)")
        skipped_count += 1
        continue
    
    print(f"[{idx}/{total_artists}] === Artist: {artist_name} ===")
    
    artist_albums = []
    
    # Lấy albums bằng phân trang (mỗi lần 20 album, max 1000)
    limit_albums = 20
    for offset in range(0, 1000, limit_albums):
        try:
            # Lấy albums của artist
            results = sp.artist_albums(
                artist_id=artist_id,
                limit=limit_albums,
                offset=offset
            )
            
            # Kiểm tra xem có dữ liệu không
            if 'items' not in results:
                print(f"  ⚠️  Không có items trong response")
                break
            
            albums = results['items']
            if not albums:
                break
            
            for album in albums:
                # Kiểm tra album có đầy đủ thông tin không
                if 'id' not in album or 'name' not in album:
                    print(f"  ⚠️  Album thiếu thông tin, bỏ qua")
                    continue
                
                # Kiểm tra album ID có trùng không
                if album['id'] not in global_seen_album_ids:
                    artist_albums.append(album)
                    global_seen_album_ids.add(album['id'])
                    release_date = album.get('release_date', 'N/A')
                    print(f"  ✓ {album['name']} ({release_date})")
                    time.sleep(0.05)
                else:
                    print(f"  ⊘ {album['name']} (trùng - bỏ qua)")
            
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
    
    # Lưu albums của artist
    all_albums_data[artist_id] = artist_albums
    processed_count += 1
    print(f"Đã lấy {len(artist_albums)} albums mới cho {artist_name}")
    
    # === GHI NGAY VÀO FILE JSON SAU MỖI ARTIST ===
    # ✅ ĐƠNGIẢN: Dùng json.dump() thông thường
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_albums_data, f, ensure_ascii=False, indent=2)
        print(f"✅ Đã ghi artist '{artist_name}' vào {output_file}\n")
    except Exception as e:
        print(f"⚠️  Lỗi khi ghi file: {e}\n")


# Tổng album (không trùng lặp)
total_albums = len(global_seen_album_ids)
print(f"\n{'='*60}")
print(f"🎉 Hoàn tất!")
print(f"   • Tổng artist cần xử lý: {total_artists}")
print(f"   • Đã xử lý: {processed_count}")
print(f"   • Bỏ qua: {skipped_count}")
print(f"   • Tổng album duy nhất: {total_albums}")
print(f"   • Dữ liệu đã lưu vào: {output_file}")
print(f"{'='*60}")
