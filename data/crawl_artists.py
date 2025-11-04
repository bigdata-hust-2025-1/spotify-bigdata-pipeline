import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json
import time
import os


# === Thiết lập client ===
client_id = "f6f46db19c494385a1e6c5076d85f742"
client_secret = "28ba343001834b51bbb830699592a47c"


sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=client_id,
    client_secret=client_secret
))


# === Lấy tất cả categories ===
all_categories = []
limit = 20
offset = 0


while True:
    res = sp.categories(limit=limit, offset=offset)
    items = res['categories']['items']
    all_categories.extend(items)
    if not res['categories']['next']:
        break
    offset += limit


print(f"Tổng số category: {len(all_categories)}")


# === Set toàn cục để đảm bảo artist là duy nhất ===
global_seen_artist_ids = set()

# === File JSON để ghi dần ===
output_file = "all_artists_by_category_unique.json"

# Khởi tạo file JSON rỗng hoặc đọc dữ liệu cũ (nếu có)
if os.path.exists(output_file):
    with open(output_file, "r", encoding="utf-8") as f:
        try:
            all_artist_data = json.load(f)
            # Nạp lại các artist ID đã có để tránh trùng lặp
            for artists_list in all_artist_data.values():
                for artist in artists_list:
                    global_seen_artist_ids.add(artist['id'])
            print(f"Đã tải {len(global_seen_artist_ids)} artist từ file cũ.")
            print(f"Đã tải {len(all_artist_data)} category từ file cũ.")
        except:
            all_artist_data = {}
else:
    all_artist_data = {}


# === Lấy artist theo từng category/genre (chỉ từ search) ===
total_categories = len(all_categories)
processed_count = 0
skipped_count = 0

for idx, cat in enumerate(all_categories, start=1):
    genre_name = cat['name'].lower()
    category_name = cat['name']
    
    # === BỎ QUA CATEGORY ĐÃ XỬ LÝ ===
    if category_name in all_artist_data:
        print(f"\n[{idx}/{total_categories}] ⏭️  Bỏ qua '{category_name}' (đã xử lý trước đó)")
        skipped_count += 1
        continue
    
    print(f"\n[{idx}/{total_categories}] === Category/Genre: {category_name} ===")
    
    category_artists = []

    # Lấy artist bằng search theo genre (phân trang để tối đa 1000 artist)
    limit_search = 50
    for offset in range(0, 1000, limit_search):
        try:
            results = sp.search(q=f"genre:{genre_name}", type="artist", limit=limit_search, offset=offset)
            artists = results['artists']['items']
            if not artists:
                break
            for artist in artists:
                # Kiểm tra trong set toàn cục
                if artist['id'] not in global_seen_artist_ids:
                    try:
                        artist_detail = sp.artist(artist['id'])
                        category_artists.append(artist_detail)
                        global_seen_artist_ids.add(artist['id'])  # Thêm vào set toàn cục
                        print(f"  ✓ {artist_detail['name']}")
                        time.sleep(0.1)
                    except:
                        continue
        except Exception as e:
            print(f"  ⚠️  Lỗi khi search: {e}")
            break

    # Lưu artist của category
    all_artist_data[category_name] = category_artists
    processed_count += 1
    print(f"Đã lấy {len(category_artists)} artist mới cho category {category_name}")
    
    # === GHI NGAY VÀO FILE JSON SAU MỖI CATEGORY ===
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_artist_data, f, ensure_ascii=False, indent=2)
    print(f"✅ Đã ghi category '{category_name}' vào {output_file}")


# Tổng artist (không trùng lặp)
total_artists = len(global_seen_artist_ids)
print(f"\n{'='*60}")
print(f"🎉 Hoàn tất!")
print(f"   • Tổng số category: {total_categories}")
print(f"   • Đã xử lý: {processed_count}")
print(f"   • Bỏ qua: {skipped_count}")
print(f"   • Tổng số artist duy nhất: {total_artists}")
print(f"   • Dữ liệu cuối cùng đã lưu vào: {output_file}")
print(f"{'='*60}")
