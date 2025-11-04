import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json
import time
import os


# === Thiết lập client ===
client_id = "04217b11389b48e58dcbd705bc1e6d23"
client_secret = "b2e4d447d3a24b7686a250790dae0dd2"

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=client_id,
    client_secret=client_secret
))


# === Đọc file flattened artists ===
input_file = "all_artists_flattened.json"
output_file = "all_playlists_by_artist.json"

with open(input_file, "r", encoding="utf-8") as f:
    all_artists = json.load(f)

print(f"Đã tải {len(all_artists)} artists từ file")

# Lấy 2000 mẫu đầu tiên
artists_to_process = all_artists[:50]
print(f"Sẽ xử lý {len(artists_to_process)} artists đầu tiên")


# === Set toàn cục để đảm bảo playlist là duy nhất ===
global_seen_playlist_ids = set()


# === Đọc file playlists cũ (nếu có) ===
all_playlists_data = {}

if os.path.exists(output_file):
    with open(output_file, "r", encoding="utf-8") as f:
        try:
            all_playlists_data = json.load(f)
            # Nạp lại playlist IDs đã có
            for artist_playlists in all_playlists_data.values():
                for playlist in artist_playlists:
                    global_seen_playlist_ids.add(playlist['id'])
            print(f"Đã tải {len(global_seen_playlist_ids)} playlists từ file cũ")
            print(f"Đã tải {len(all_playlists_data)} artists từ file cũ")
        except Exception as e:
            print(f"Lỗi khi đọc file cũ: {e}")
            all_playlists_data = {}
else:
    all_playlists_data = {}


# === Lấy playlists theo từng artist ===
total_artists = len(artists_to_process)
processed_count = 0
skipped_count = 0
none_count = 0  # Track số lượng None items


for idx, artist in enumerate(artists_to_process, start=1):
    artist_id = artist['id']
    artist_name = artist['name']
    
    # Bỏ qua artist đã xử lý
    if artist_id in all_playlists_data:
        print(f"[{idx}/{total_artists}] ⏭️  Bỏ qua '{artist_name}' (đã xử lý)")
        skipped_count += 1
        continue
    
    print(f"[{idx}/{total_artists}] === Artist: {artist_name} ===")
    
    artist_playlists = []
    
    # === SEARCH PLAYLISTS CHỨA ARTIST (với pagination) ===
    limit_playlists = 50  # Max 50 per request
    
    for offset in range(0, 1000, limit_playlists):
        try:
            # Search playlists
            search_results = sp.search(
                q=artist_name,
                type='playlist',
                limit=limit_playlists,
                offset=offset
            )
            
            # ✅ FIX: Kiểm tra search_results không null
            if not search_results:
                print(f"  ⚠️  Search result rỗng, dừng pagination")
                break
            
            # ✅ FIX: Kiểm tra playlists key tồn tại
            if 'playlists' not in search_results or not search_results['playlists']:
                print(f"  ⚠️  Không có playlists key trong response, dừng")
                break
            
            playlists_obj = search_results['playlists']
            
            # ✅ FIX: Kiểm tra items tồn tại
            if 'items' not in playlists_obj or not playlists_obj['items']:
                print(f"  ⏹️  Hết playlists ở offset {offset}")
                break
            
            playlists = playlists_obj['items']
            
            # ========== ✅ FIX CHÍNH: SKIP NONE ITEMS ==========
            for playlist_item in playlists:
                # ⭐ SKIP NẾU ITEM LÀ NONE
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
                if playlist_id not in global_seen_playlist_ids:
                    try:
                        # ✅ CẢI TIẾN: Không gọi sp.playlist() nữa
                        # Chỉ lưu thông tin có sẵn từ search results
                        
                        playlist_brief = {
                            "id": playlist_item.get("id"),
                            "name": playlist_item.get("name"),
                            "owner": playlist_item.get("owner", {}),
                            "description": playlist_item.get("description", ""),
                            "images": playlist_item.get("images", []),
                            "tracks": playlist_item.get("tracks", {}),
                            "external_urls": playlist_item.get("external_urls", {}),
                            "public": playlist_item.get("public"),
                            "collaborative": playlist_item.get("collaborative"),
                            "uri": playlist_item.get("uri"),
                            "href": playlist_item.get("href"),
                            "type": playlist_item.get("type")
                        }
                        
                        # Lưu playlist
                        artist_playlists.append(playlist_brief)
                        global_seen_playlist_ids.add(playlist_id)
                        
                        owner_name = playlist_brief.get('owner', {}).get('display_name', 'Unknown')
                        tracks_total = playlist_brief.get('tracks', {}).get('total', 0)
                        
                        print(f"  ✓ {playlist_brief['name']} by {owner_name} ({tracks_total} tracks)")
                        time.sleep(0.05)
                        
                    except Exception as e:
                        print(f"  ⚠️  Lỗi khi xử lý playlist {playlist_id}: {e}")
                        time.sleep(0.5)
                        continue
                else:
                    print(f"  ⊘ {playlist_name} (trùng - bỏ qua)")
            
            # ✅ FIX: Kiểm tra 'next' một cách an toàn
            next_url = playlists_obj.get('next')
            if not next_url:  # None hoặc empty string
                print(f"  ⏹️  Không có trang tiếp theo")
                break
            
            time.sleep(0.2)
                
        except spotipy.exceptions.SpotifyException as e:
            print(f"  ⚠️  Lỗi Spotify API khi search: {e}")
            if e.http_status == 429:
                print(f"  ⏸️  Chờ 60 giây do rate limit...")
                time.sleep(60)
                continue
            else:
                time.sleep(1)
                break
        except Exception as e:
            print(f"  ⚠️  Lỗi không xác định khi search: {e}")
            # Log chi tiết hơn
            import traceback
            print(f"  Error details: {traceback.format_exc()}")
            time.sleep(1)
            break
    
    # Lưu playlists của artist
    all_playlists_data[artist_id] = artist_playlists
    processed_count += 1
    print(f"Đã lấy {len(artist_playlists)} playlists mới cho {artist_name}")
    
    # === GHI NGAY VÀO FILE JSON SAU MỖI ARTIST ===
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_playlists_data, f, ensure_ascii=False, indent=2)
        print(f"✅ Đã ghi artist '{artist_name}' vào {output_file}\n")
    except Exception as e:
        print(f"⚠️  Lỗi khi ghi file: {e}\n")


# Tổng playlists (không trùng lặp)
total_playlists = len(global_seen_playlist_ids)
print(f"\n{'='*60}")
print(f"🎉 Hoàn tát!")
print(f"   • Tổng artists cần xử lý: {total_artists}")
print(f"   • Đã xử lý: {processed_count}")
print(f"   • Bỏ qua: {skipped_count}")
print(f"   • Tổng playlists duy nhất: {total_playlists}")
print(f"   • Số lượng None items bỏ qua: {none_count}")
print(f"   • Dữ liệu đã lưu vào: {output_file}")
print(f"{'='*60}")