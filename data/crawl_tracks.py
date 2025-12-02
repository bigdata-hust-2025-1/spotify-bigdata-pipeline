import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json
import time
import os


if os.path.exists(".cache"):
    os.remove(".cache")
    print("Đã xóa file .cache cũ")


# === Thiết lập client ===
# Key 11:
client_id = "e841b804e1834a668bcaddd62e076a17"
client_secret = "ec1827316a744cb7a0d7c474ed8b363e"


sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=client_id,
    client_secret=client_secret
))


# === Đọc file flattened albums ===
input_file = "albums.json"
output_file = "tracks.json"
metadata_file = "tracks_processing_metadata.json"  # ✅ File metadata


with open(input_file, "r", encoding="utf-8") as f:
    all_albums = json.load(f)


print(f"Đã tải {len(all_albums)} albums từ file")


# ✅ Lấy TOÀN BỘ albums (không giới hạn)
albums_to_process = all_albums
print(f"Sẽ xử lý {len(albums_to_process)} albums")




# === Set toàn cục để đảm bảo track là duy nhất ===
global_seen_track_ids = set()


# === Đọc file tracks cũ (nếu có) ===
all_tracks_data = []  # ✅ Thay đổi: dùng list thay vì dict


if os.path.exists(output_file):
    with open(output_file, "r", encoding="utf-8") as f:
        try:
            all_tracks_data = json.load(f)
            # Nạp lại track IDs đã có
            for track in all_tracks_data:
                global_seen_track_ids.add(track['id'])
            print(f"Đã tải {len(global_seen_track_ids)} tracks từ file cũ")
        except Exception as e:
            print(f"Lỗi khi đọc file cũ: {e}")
            all_tracks_data = []
else:
    all_tracks_data = []


# === ✅ ĐỌC FILE METADATA (ALBUM ĐÃ XỬ LÝ) ===
processed_album_ids = set()

if os.path.exists(metadata_file):
    with open(metadata_file, "r", encoding="utf-8") as f:
        try:
            metadata = json.load(f)
            processed_album_ids = set(metadata.get("processed_albums", []))
            print(f"📋 Đã tải {len(processed_album_ids)} albums đã xử lý từ metadata")
            
            # Hiển thị thông tin thêm nếu có
            if "last_update" in metadata:
                print(f"   Cập nhật lần cuối: {metadata['last_update']}")
            if "total_tracks" in metadata:
                print(f"   Tổng tracks trong metadata: {metadata['total_tracks']}")
        except Exception as e:
            print(f"⚠️  Lỗi khi đọc metadata: {e}")
            processed_album_ids = set()
else:
    print("📋 Không tìm thấy file metadata, bắt đầu từ đầu")
    processed_album_ids = set()




# === Lấy tracks theo từng album ===
total_albums = len(albums_to_process)
processed_count = 0
skipped_count = 0
new_tracks_count = 0


for idx, album in enumerate(albums_to_process, start=1):
    album_id = album['id']
    album_name = album['name']
    album_release_date = album.get('release_date', 'N/A')  # ✅ Lấy release_date từ album
    
    # === ✅ KIỂM TRA ALBUM ĐÃ XỬ LÝ CHƯA ===
    if album_id in processed_album_ids:
        print(f"[{idx}/{total_albums}] ⏭️  Bỏ qua '{album_name}' (đã xử lý)")
        skipped_count += 1
        continue
    
    print(f"[{idx}/{total_albums}] === Album: {album_name} ===")
    
    # ✅ Lấy tối đa 10 tracks của album
    try:
        results = sp.album_tracks(
            album_id=album_id,
            limit=10,  # ✅ Chỉ lấy 10 tracks
            offset=0
        )
        
        # Kiểm tra xem có dữ liệu không
        if 'items' not in results:
            print(f"  ⚠️  Không có items trong response")
            # ✅ Vẫn đánh dấu là đã xử lý
            processed_album_ids.add(album_id)
            processed_count += 1
            continue
        
        tracks = results['items']
        if not tracks:
            print(f"  ⚠️  Album không có track nào")
            # ✅ Vẫn đánh dấu là đã xử lý
            processed_album_ids.add(album_id)
            processed_count += 1
            continue
        
        print(f"  Tìm thấy {len(tracks)} tracks")
        
        # Xử lý từng track
        for track in tracks:
            # Kiểm tra track có đầy đủ thông tin không
            if 'id' not in track:
                print(f"  ⚠️  Track thiếu ID, bỏ qua")
                continue
            
            track_id = track['id']
            
            # Kiểm tra track ID có trùng không
            if track_id in global_seen_track_ids:
                print(f"  ⊘ {track.get('name', 'Unknown')} (trùng - bỏ qua)")
                continue
            
            # === ✅ LẤY THÔNG TIN CHI TIẾT CỦA TRACK ===
            try:
                print(f"  🔍 Đang lấy chi tiết track: {track.get('name', 'Unknown')}")
                track_details = sp.track(track_id)
                time.sleep(0.1)  # Tránh rate limit
                
                # === ✅ TỐI GIẢN TRACK THEO CẤU TRÚC YÊU CẦU ===
                simplified_track = {
                    "album": {
                        "id": track_details.get("album", {}).get("id")
                    },
                    "artists": [
                        {"id": artist_item.get("id")}
                        for artist_item in track_details.get("artists", [])
                    ],
                    "disc_number": track_details.get("disc_number"),
                    "duration_ms": track_details.get("duration_ms"),
                    "explicit": track_details.get("explicit"),
                    "release_date": album_release_date,  # ✅ Lấy từ album
                    "id": track_details.get("id"),
                    "is_local": track_details.get("is_local"),
                    "name": track_details.get("name"),
                    "popularity": track_details.get("popularity"),
                    "track_number": track_details.get("track_number"),
                    "type": track_details.get("type")
                }
                
                # Thêm vào danh sách và set
                all_tracks_data.append(simplified_track)
                global_seen_track_ids.add(track_id)
                new_tracks_count += 1
                
                duration_min = simplified_track['duration_ms'] / 1000 / 60 if simplified_track['duration_ms'] else 0
                print(f"  ✓ {simplified_track['name']} ({duration_min:.2f} min, popularity: {simplified_track['popularity']})")
                
            except spotipy.exceptions.SpotifyException as e:
                print(f"  ⚠️  Lỗi khi lấy chi tiết track: {e}")
                if e.http_status == 429:  # Rate limit
                    print(f"  ⏸️  Chờ 60 giây do rate limit...")
                    time.sleep(60)
                    continue
                else:
                    time.sleep(1)
                    continue
            except Exception as e:
                print(f"  ⚠️  Lỗi không xác định: {e}")
                time.sleep(1)
                continue
        
        # === ✅ ĐÁNH DẤU ALBUM ĐÃ XỬ LÝ XONG ===
        processed_album_ids.add(album_id)
        processed_count += 1
        print(f"Đã xử lý xong album '{album_name}'\n")
        
        # === GHI VÀO FILE SAU MỖI 50 ALBUMS ===
        if processed_count % 50 == 0:
            try:
                # Ghi file tracks
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(all_tracks_data, f, ensure_ascii=False, indent=2)
                
                # === ✅ GHI FILE METADATA ===
                with open(metadata_file, "w", encoding="utf-8") as f:
                    metadata = {
                        "processed_albums": list(processed_album_ids),
                        "total_tracks": len(global_seen_track_ids),
                        "last_update": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "total_processed": processed_count,
                        "total_skipped": skipped_count
                    }
                    json.dump(metadata, f, ensure_ascii=False, indent=2)
                
                print(f"💾 Đã lưu checkpoint tại album thứ {processed_count} (+ metadata)\n")
            except Exception as e:
                print(f"⚠️  Lỗi khi ghi file: {e}\n")
                
    except spotipy.exceptions.SpotifyException as e:
        print(f"  ⚠️  Lỗi Spotify API khi lấy tracks: {e}")
        if e.http_status == 429:  # Rate limit
            print(f"  ⏸️  Chờ 60 giây do rate limit...")
            time.sleep(60)
            continue
        else:
            # ✅ Đánh dấu album bị lỗi để không thử lại
            processed_album_ids.add(album_id)
            processed_count += 1
            time.sleep(1)
            continue
    except Exception as e:
        print(f"  ⚠️  Lỗi không xác định: {e}")
        # ✅ Đánh dấu album bị lỗi
        processed_album_ids.add(album_id)
        processed_count += 1
        time.sleep(1)
        continue


# === GHI FILE CUỐI CÙNG ===
try:
    # Ghi tracks
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_tracks_data, f, ensure_ascii=False, indent=2)
    print(f"✅ Đã ghi file tracks cuối cùng vào {output_file}")
    
    # === ✅ GHI METADATA CUỐI CÙNG ===
    with open(metadata_file, "w", encoding="utf-8") as f:
        metadata = {
            "processed_albums": list(processed_album_ids),
            "total_tracks": len(global_seen_track_ids),
            "last_update": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_processed": processed_count,
            "total_skipped": skipped_count,
            "status": "completed"
        }
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    print(f"✅ Đã ghi file metadata cuối cùng vào {metadata_file}")
    
except Exception as e:
    print(f"⚠️  Lỗi khi ghi file cuối cùng: {e}")


# Tổng tracks (không trùng lặp)
total_tracks = len(global_seen_track_ids)
print(f"\n{'='*60}")
print(f"🎉 Hoàn tất!")
print(f"   • Tổng albums cần xử lý: {total_albums}")
print(f"   • Đã xử lý: {processed_count}")
print(f"   • Đã bỏ qua: {skipped_count}")
print(f"   • Tracks mới thêm vào: {new_tracks_count}")
print(f"   • Tổng tracks duy nhất: {total_tracks}")
print(f"   • Dữ liệu tracks: {output_file}")
print(f"   • Dữ liệu metadata: {metadata_file}")
print(f"{'='*60}")
