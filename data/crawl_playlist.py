import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json
import time
import os


if os.path.exists(".cache"):
    os.remove(".cache")
    print("Đã xóa file .cache cũ")


# === Thiết lập client ===
client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")



sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=client_id,
    client_secret=client_secret
))


# === Đọc file artists và metadata ===
artists_file = "artists.json"
albums_metadata_file = "albums_processing_metadata.json"
output_file = "playlists.json"
playlists_metadata_file = "playlists_processing_metadata.json"



# ✅ ĐỌC DANH SÁCH ARTIST TỪ ALBUMS METADATA
with open(albums_metadata_file, "r", encoding="utf-8") as f:
    albums_metadata = json.load(f)
    processed_artist_ids_from_albums = albums_metadata.get("processed_artists", [])


print(f"Đã tải {len(processed_artist_ids_from_albums)} artists từ albums metadata")



# Load thông tin chi tiết artists
with open(artists_file, "r", encoding="utf-8") as f:
    all_artists = json.load(f)


# ✅ Lọc artists theo IDs từ metadata
artists_to_process = [
    artist for artist in all_artists 
    if artist['id'] in processed_artist_ids_from_albums
]


print(f"Sẽ xử lý {len(artists_to_process)} artists từ albums metadata")




# === Set toàn cục để đảm bảo playlist là duy nhất ===
global_seen_playlist_ids = set()



# === Đọc file playlists cũ (nếu có) ===
all_playlists_data = []


if os.path.exists(output_file):
    with open(output_file, "r", encoding="utf-8") as f:
        try:
            all_playlists_data = json.load(f)
            for playlist in all_playlists_data:
                global_seen_playlist_ids.add(playlist['id'])
            print(f"Đã tải {len(global_seen_playlist_ids)} playlists từ file cũ")
        except Exception as e:
            print(f"Lỗi khi đọc file cũ: {e}")
            all_playlists_data = []
else:
    all_playlists_data = []



# === ✅ ĐỌC FILE METADATA ===
processed_artist_ids = set()


if os.path.exists(playlists_metadata_file):
    with open(playlists_metadata_file, "r", encoding="utf-8") as f:
        try:
            metadata = json.load(f)
            processed_artist_ids = set(metadata.get("processed_artists", []))
            print(f"📋 Đã tải {len(processed_artist_ids)} artists đã xử lý từ playlists metadata")
            
            if "last_update" in metadata:
                print(f"   Cập nhật lần cuối: {metadata['last_update']}")
            if "total_playlists" in metadata:
                print(f"   Tổng playlists trong metadata: {metadata['total_playlists']}")
        except Exception as e:
            print(f"⚠️  Lỗi khi đọc metadata: {e}")
            processed_artist_ids = set()
else:
    print("📋 Không tìm thấy file playlists metadata, bắt đầu từ đầu")
    processed_artist_ids = set()




# === Lấy playlists theo từng artist ===
total_artists = len(artists_to_process)
processed_count = 0
skipped_count = 0
new_playlists_count = 0
none_count = 0



for idx, artist in enumerate(artists_to_process, start=1):
    artist_id = artist['id']
    artist_name = artist['name']
    
    # === ✅ KIỂM TRA ARTIST ĐÃ XỬ LÝ CHƯA ===
    if artist_id in processed_artist_ids:
        print(f"[{idx}/{total_artists}] ⏭️  Bỏ qua '{artist_name}' (đã xử lý)")
        skipped_count += 1
        continue
    
    print(f"[{idx}/{total_artists}] === Artist: {artist_name} ===")
    
    # === ✅ SEARCH 1 LẦN DUY NHẤT VỚI LIMIT=10 ===
    try:
        search_results = sp.search(
            q=artist_name,
            type='playlist',
            limit=10,  # ✅ CHỈ LẤY 10 PLAYLISTS
            offset=0   # ✅ CHỈ LẤY PAGE ĐẦU TIÊN
        )
        
        # Kiểm tra search_results
        if not search_results or 'playlists' not in search_results:
            print(f"  ⚠️  Search result không hợp lệ")
            processed_artist_ids.add(artist_id)
            processed_count += 1
            continue
        
        playlists_obj = search_results['playlists']
        
        if 'items' not in playlists_obj or not playlists_obj['items']:
            print(f"  ⚠️  Không tìm thấy playlists")
            processed_artist_ids.add(artist_id)
            processed_count += 1
            continue
        
        playlists = playlists_obj['items']
        print(f"  Tìm thấy {len(playlists)} playlists")
        
        # === Xử lý từng playlist ===
        for playlist_item in playlists:
            # Skip None items
            if playlist_item is None:
                none_count += 1
                print(f"  ⊘ None item (bỏ qua)")
                continue
            
            # Kiểm tra playlist có đầy đủ thông tin không
            if 'id' not in playlist_item or 'name' not in playlist_item:
                print(f"  ⚠️  Playlist thiếu thông tin, bỏ qua")
                continue
            
            playlist_id = playlist_item['id']
            playlist_name = playlist_item['name']
            
            # Kiểm tra playlist ID có trùng không
            if playlist_id in global_seen_playlist_ids:
                print(f"  ⊘ {playlist_name} (trùng - bỏ qua)")
                continue
            
            # === ✅ LẤY THÔNG TIN CHI TIẾT CỦA PLAYLIST ===
            try:
                print(f"  🔍 Đang lấy chi tiết playlist: {playlist_name}")
                playlist_info = sp.playlist(playlist_id)
                time.sleep(0.1)  # Tránh rate limit
                
                # === TỐI GIẢN PLAYLIST ===
                simplified_playlist = {
                    "collaborative": playlist_info.get("collaborative"),
                    "description": playlist_info.get("description"),
                    "followers": {
                        "href": playlist_info.get("followers", {}).get("href"),
                        "total": playlist_info.get("followers", {}).get("total")
                    },
                    "id": playlist_info.get("id"),
                    "name": playlist_info.get("name"),
                    "owner": {
                        "display_name": playlist_info.get("owner", {}).get("display_name"),
                        "id": playlist_info.get("owner", {}).get("id"),
                        "type": playlist_info.get("owner", {}).get("type")
                    },
                    "primary_color": playlist_info.get("primary_color"),
                    "public": playlist_info.get("public"),
                    "snapshot_id": playlist_info.get("snapshot_id"),
                    "type": playlist_info.get("type")
                }
                
                # Thêm vào danh sách
                all_playlists_data.append(simplified_playlist)
                global_seen_playlist_ids.add(playlist_id)
                new_playlists_count += 1
                
                owner_name = simplified_playlist['owner'].get('display_name', 'Unknown')
                followers = simplified_playlist['followers'].get('total', 0)
                
                print(f"  ✓ {simplified_playlist['name']} by {owner_name} ({followers} followers)")
                
            except spotipy.exceptions.SpotifyException as e:
                print(f"  ⚠️  Lỗi khi lấy chi tiết playlist: {e}")
                if e.http_status == 429:
                    print(f"  ⏸️  Chờ 60 giây do rate limit...")
                    time.sleep(60)
                else:
                    time.sleep(1)
            except Exception as e:
                print(f"  ⚠️  Lỗi: {e}")
                time.sleep(1)
        
        # ✅ ĐÁNH DẤU ARTIST ĐÃ XỬ LÝ
        processed_artist_ids.add(artist_id)
        processed_count += 1
        print(f"Đã xử lý xong artist '{artist_name}'\n")
        
        # === GHI VÀO FILE SAU MỖI 10 ARTISTS ===
        if processed_count % 10 == 0:
            try:
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(all_playlists_data, f, ensure_ascii=False, indent=2)
                
                with open(playlists_metadata_file, "w", encoding="utf-8") as f:
                    metadata = {
                        "processed_artists": list(processed_artist_ids),
                        "total_playlists": len(global_seen_playlist_ids),
                        "last_update": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "total_processed": processed_count,
                        "total_skipped": skipped_count
                    }
                    json.dump(metadata, f, ensure_ascii=False, indent=2)
                
                print(f"💾 Đã lưu checkpoint tại artist thứ {processed_count} (+ metadata)\n")
            except Exception as e:
                print(f"⚠️  Lỗi khi ghi file: {e}\n")
                
    except spotipy.exceptions.SpotifyException as e:
        print(f"  ⚠️  Lỗi Spotify API: {e}")
        if e.http_status == 429:
            print(f"  ⏸️  Chờ 60 giây do rate limit...")
            time.sleep(60)
        else:
            time.sleep(1)
        processed_artist_ids.add(artist_id)
        processed_count += 1
    except Exception as e:
        print(f"  ⚠️  Lỗi không xác định: {e}")
        import traceback
        print(f"  Error details: {traceback.format_exc()}")
        time.sleep(1)
        processed_artist_ids.add(artist_id)
        processed_count += 1



# === GHI FILE CUỐI CÙNG ===
try:
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_playlists_data, f, ensure_ascii=False, indent=2)
    print(f"✅ Đã ghi file playlists cuối cùng vào {output_file}")
    
    with open(playlists_metadata_file, "w", encoding="utf-8") as f:
        metadata = {
            "processed_artists": list(processed_artist_ids),
            "total_playlists": len(global_seen_playlist_ids),
            "last_update": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_processed": processed_count,
            "total_skipped": skipped_count,
            "status": "completed"
        }
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    print(f"✅ Đã ghi file metadata cuối cùng vào {playlists_metadata_file}")
    
except Exception as e:
    print(f"⚠️  Lỗi khi ghi file cuối cùng: {e}")


# Tổng playlists
total_playlists = len(global_seen_playlist_ids)
print(f"\n{'='*60}")
print(f"🎉 Hoàn tất!")
print(f"   • Tổng artists cần xử lý: {total_artists}")
print(f"   • Đã xử lý: {processed_count}")
print(f"   • Đã bỏ qua: {skipped_count}")
print(f"   • Playlists mới thêm vào: {new_playlists_count}")
print(f"   • Tổng playlists duy nhất: {total_playlists}")
print(f"   • Số lượng None items: {none_count}")
print(f"   • Giới hạn playlists/artist: 10")
print(f"   • Dữ liệu playlists: {output_file}")
print(f"   • Dữ liệu metadata: {playlists_metadata_file}")
print(f"{'='*60}")
