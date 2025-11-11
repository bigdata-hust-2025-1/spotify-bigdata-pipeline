import json

# Đọc file playlists
input_file = "playlists.json"
output_file = "owners.json"

with open(input_file, "r", encoding="utf-8") as f:
    playlists = json.load(f)

print(f"Đã tải {len(playlists)} playlists từ file")

# === Set để tracking owner IDs (tránh trùng lặp) ===
seen_owner_ids = set()

# === List chứa owners duy nhất ===
unique_owners = []

# Duyệt qua từng playlist
for playlist in playlists:
    # Kiểm tra playlist có owner không
    if 'owner' not in playlist or not playlist['owner']:
        continue
    
    owner = playlist['owner']
    
    # Kiểm tra owner có id không
    if 'id' not in owner or not owner['id']:
        continue
    
    owner_id = owner['id']
    
    # Nếu chưa thấy owner này → Thêm vào list
    if owner_id not in seen_owner_ids:
        unique_owners.append(owner)
        seen_owner_ids.add(owner_id)

# Ghi vào file mới
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(unique_owners, f, ensure_ascii=False, indent=2)

print(f"\n{'='*60}")
print(f"✅ Hoàn tất!")
print(f"   • Tổng playlists: {len(playlists)}")
print(f"   • Owners duy nhất: {len(unique_owners)}")
print(f"   • Owners trùng lặp bị loại: {len(playlists) - len(unique_owners)}")
print(f"   • Dữ liệu đã lưu vào: {output_file}")
print(f"{'='*60}")
