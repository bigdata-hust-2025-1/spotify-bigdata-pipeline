import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json
import time
import os
import random




# === Thiết lập client ===
# Key 6:
client_id = "feafd61861a3429bb2ab1654006da13b"
client_secret = "a35230765c0b46abb5c10c0fe99e64c4"


sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=client_id,
    client_secret=client_secret
))




# === Đọc file flattened artists ===
input_file = "artists.json"
output_file = "albums.json"
metadata_file = "albums_processing_metadata.json"  # ✅ File metadata



with open(input_file, "r", encoding="utf-8") as f:
    all_artists = json.load(f)



print(f"Đã tải {len(all_artists)} artist từ file")



# Lấy 5000 artist ngẫu nhiên
random.seed(42)  # Để kết quả reproducible
artists_to_process = random.sample(all_artists, min(5000, len(all_artists)))
print(f"Đã chọn ngẫu nhiên {len(artists_to_process)} artist để xử lý")




# === Set toàn cục để đảm bảo album là duy nhất ===
global_seen_album_ids = set()



# === Đọc file albums cũ (nếu có) ===
all_albums_data = []  # Thay đổi: dùng list thay vì dict



if os.path.exists(output_file):
    with open(output_file, "r", encoding="utf-8") as f:
        try:
            all_albums_data = json.load(f)
            # Nạp lại album IDs đã có
            for album in all_albums_data:
                global_seen_album_ids.add(album['id'])
            print(f"Đã tải {len(global_seen_album_ids)} albums từ file cũ")
        except Exception as e:
            print(f"Lỗi khi đọc file cũ: {e}")
            all_albums_data = []
else:
    all_albums_data = []



# === ✅ ĐỌC FILE METADATA (ARTIST ĐÃ XỬ LÝ) ===
processed_artist_ids = set()

if os.path.exists(metadata_file):
    with open(metadata_file, "r", encoding="utf-8") as f:
        try:
            metadata = json.load(f)
            processed_artist_ids = set(metadata.get("processed_artists", []))
            print(f"📋 Đã tải {len(processed_artist_ids)} artists đã xử lý từ metadata")
            
            # Hiển thị thông tin thêm nếu có
            if "last_update" in metadata:
                print(f"   Cập nhật lần cuối: {metadata['last_update']}")
            if "total_albums" in metadata:
                print(f"   Tổng albums trong metadata: {metadata['total_albums']}")
        except Exception as e:
            print(f"⚠️  Lỗi khi đọc metadata: {e}")
            processed_artist_ids = set()
else:
    print("📋 Không tìm thấy file metadata, bắt đầu từ đầu")
    processed_artist_ids = set()




# === Lấy albums theo từng artist ===
total_artists = len(artists_to_process)
processed_count = 0
skipped_count = 0
new_albums_count = 0



for idx, artist in enumerate(artists_to_process, start=1):
    artist_id = artist['id']
    artist_name = artist['name']
    
    # === ✅ KIỂM TRA ARTIST ĐÃ XỬ LÝ CHƯA ===
    if artist_id in processed_artist_ids:
        print(f"[{idx}/{total_artists}] ⏭️  Bỏ qua '{artist_name}' (đã xử lý)")
        skipped_count += 1
        continue
    
    print(f"[{idx}/{total_artists}] === Artist: {artist_name} ===")
    
    # Lấy tối đa 5 albums của artist
    try:
        results = sp.artist_albums(
            artist_id=artist_id,
            limit=5,  # Chỉ lấy 5 albums
            offset=0
        )
        
        # Kiểm tra xem có dữ liệu không
        if 'items' not in results:
            print(f"  ⚠️  Không có items trong response")
            # ✅ Vẫn đánh dấu là đã xử lý để không thử lại
            processed_artist_ids.add(artist_id)
            processed_count += 1
            continue
        
        albums = results['items']
        if not albums:
            print(f"  ⚠️  Artist không có album nào")
            # ✅ Vẫn đánh dấu là đã xử lý
            processed_artist_ids.add(artist_id)
            processed_count += 1
            continue
        
        print(f"  Tìm thấy {len(albums)} albums")
        
        # Xử lý từng album
        for album in albums:
            # Kiểm tra album có đầy đủ thông tin không
            if 'id' not in album:
                print(f"  ⚠️  Album thiếu ID, bỏ qua")
                continue
            
            album_id = album['id']
            
            # Kiểm tra album ID có trùng không
            if album_id in global_seen_album_ids:
                print(f"  ⊘ {album.get('name', 'Unknown')} (trùng - bỏ qua)")
                continue
            
            # === LẤY THÔNG TIN CHI TIẾT CỦA ALBUM ===
            try:
                print(f"  🔍 Đang lấy chi tiết album: {album.get('name', 'Unknown')}")
                album_details = sp.album(album_id)
                time.sleep(0.1)  # Tránh rate limit
                
                # === TỐI GIẢN ALBUM THEO CẤU TRÚC YÊU CẦU ===
                simplified_album = {
                    "album_type": album_details.get("album_type"),
                    "total_tracks": album_details.get("total_tracks"),
                    "id": album_details.get("id"),
                    "name": album_details.get("name"),
                    "release_date": album_details.get("release_date"),
                    "type": album_details.get("type"),
                    "artists": [
                        {"id": artist_item.get("id")}
                        for artist_item in album_details.get("artists", [])
                    ],
                    "copyrights": [
                        {
                            "text": copyright_item.get("text"),
                            "type": copyright_item.get("type")
                        }
                        for copyright_item in album_details.get("copyrights", [])
                    ],
                    "genres": album_details.get("genres", []),
                    "label": album_details.get("label"),
                    "popularity": album_details.get("popularity")
                }
                
                # Thêm vào danh sách và set
                all_albums_data.append(simplified_album)
                global_seen_album_ids.add(album_id)
                new_albums_count += 1
                
                print(f"  ✓ {simplified_album['name']} (popularity: {simplified_album['popularity']})")
                
            except spotipy.exceptions.SpotifyException as e:
                print(f"  ⚠️  Lỗi khi lấy chi tiết album: {e}")
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
        
        # === ✅ ĐÁNH DẤU ARTIST ĐÃ XỬ LÝ XONG ===
        processed_artist_ids.add(artist_id)
        processed_count += 1
        print(f"Đã xử lý xong artist '{artist_name}'\n")
        
        # === GHI VÀO FILE SAU MỖI 10 ARTISTS ===
        if processed_count % 10 == 0:
            try:
                # Ghi file albums
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(all_albums_data, f, ensure_ascii=False, indent=2)
                
                # === ✅ GHI FILE METADATA ===
                with open(metadata_file, "w", encoding="utf-8") as f:
                    metadata = {
                        "processed_artists": list(processed_artist_ids),
                        "total_albums": len(global_seen_album_ids),
                        "last_update": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "total_processed": processed_count,
                        "total_skipped": skipped_count
                    }
                    json.dump(metadata, f, ensure_ascii=False, indent=2)
                
                print(f"💾 Đã lưu checkpoint tại artist thứ {processed_count} (+ metadata)\n")
            except Exception as e:
                print(f"⚠️  Lỗi khi ghi file: {e}\n")
                
    except spotipy.exceptions.SpotifyException as e:
        print(f"  ⚠️  Lỗi Spotify API khi lấy albums: {e}")
        if e.http_status == 429:  # Rate limit
            print(f"  ⏸️  Chờ 60 giây do rate limit...")
            time.sleep(60)
            continue
        else:
            # ✅ Đánh dấu artist bị lỗi để không thử lại
            processed_artist_ids.add(artist_id)
            processed_count += 1
            time.sleep(1)
            continue
    except Exception as e:
        print(f"  ⚠️  Lỗi không xác định: {e}")
        # ✅ Đánh dấu artist bị lỗi
        processed_artist_ids.add(artist_id)
        processed_count += 1
        time.sleep(1)
        continue



# === GHI FILE CUỐI CÙNG ===
try:
    # Ghi albums
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_albums_data, f, ensure_ascii=False, indent=2)
    print(f"✅ Đã ghi file albums cuối cùng vào {output_file}")
    
    # === ✅ GHI METADATA CUỐI CÙNG ===
    with open(metadata_file, "w", encoding="utf-8") as f:
        metadata = {
            "processed_artists": list(processed_artist_ids),
            "total_albums": len(global_seen_album_ids),
            "last_update": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_processed": processed_count,
            "total_skipped": skipped_count,
            "status": "completed"
        }
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    print(f"✅ Đã ghi file metadata cuối cùng vào {metadata_file}")
    
except Exception as e:
    print(f"⚠️  Lỗi khi ghi file cuối cùng: {e}")



# Tổng album (không trùng lặp)
total_albums = len(global_seen_album_ids)
print(f"\n{'='*60}")
print(f"🎉 Hoàn tất!")
print(f"   • Tổng artist cần xử lý: {total_artists}")
print(f"   • Đã xử lý: {processed_count}")
print(f"   • Đã bỏ qua: {skipped_count}")
print(f"   • Albums mới thêm vào: {new_albums_count}")
print(f"   • Tổng album duy nhất: {total_albums}")
print(f"   • Dữ liệu albums: {output_file}")
print(f"   • Dữ liệu metadata: {metadata_file}")
print(f"{'='*60}")
