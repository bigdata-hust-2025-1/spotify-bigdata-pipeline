import json
import time
import random
import os
import uuid
from kafka import KafkaProducer

# Cấu hình Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
<<<<<<< HEAD
TOPIC_NAME = 'spotify_playback_events'  # Đổi tên topic cho đúng ngữ nghĩa
DATA_FILE_PATH = r"D:\semester2025.1\BigData\project\spotify-bigdata-pipeline\data\tracks.json"
=======
TOPIC_NAME = 'spotify_playback_events'
DATA_FILE_PATH = r"D:\Big_Data_For_School\data\tracks.json"
>>>>>>> de7d1de (Add streaming jobs and update batch & kafka configs)

# Cấu hình giả lập
LOCATIONS = ['VN', 'US', 'UK', 'KR', 'JP', 'DE']
DEVICES = ['mobile', 'desktop', 'tablet', 'smart_speaker']

def create_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    except Exception as e:
        print(f"❌ Lỗi kết nối Kafka: {e}"); return None

def load_tracks():
    if not os.path.exists(DATA_FILE_PATH):
        print(f"❌ Không tìm thấy file: {DATA_FILE_PATH}"); return []
    with open(DATA_FILE_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
        return data if isinstance(data, list) else [data]

def generate_event(track):
    """Tạo ra một sự kiện nghe nhạc từ thông tin bài hát"""
    
    # 1. Logic giả lập thời gian nghe
    total_duration = track.get('duration_ms', 0)
    
    # --- [FIX LỖI] Xử lý bài hát quá ngắn (< 10s) ---
    if total_duration <= 10000:
        # Nếu bài ngắn hơn 10s thì coi như nghe hết bài
        listened_ms = total_duration
    else:
        # Nếu bài dài thì random từ 10s đến hết bài
        listened_ms = random.randint(10000, total_duration)
    # ------------------------------------------------
    
    # Xác định trạng thái: Nếu nghe > 80% thời lượng thì coi là 'completed'
    # Lưu ý: Chia cho 0 sẽ lỗi nên cần check total_duration > 0
    if total_duration > 0 and listened_ms >= (total_duration * 0.5):
        status = 'completed'
    else:
        status = 'skipped'

    # 2. Tạo bản tin Event
    event = {
        # --- Metadata (Lấy từ tracks.json) ---
        "track_id": track.get('id'),
        "track_name": track.get('name'),
        "artist_id": track['artists'][0]['id'] if track.get('artists') else None,
        "album_id": track['album']['id'] if track.get('album') else None,
        "track_popularity": track.get('popularity'),
        
        # --- Event Data (Giả lập) ---
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 500)}", 
        "timestamp": time.time(),
        "event_time_str": time.strftime("%Y-%m-%d %H:%M:%S"),
        "location": random.choice(LOCATIONS),
        "device": random.choice(DEVICES),
        "listen_duration_ms": listened_ms,
        "status": status
    }
    return event

def main():
    tracks = load_tracks()
    if not tracks: return

    producer = create_producer()
    if not producer: return

    print(f"🚀 Bắt đầu Streaming sự kiện nghe nhạc vào '{TOPIC_NAME}'...")
    print("Log mẫu: [User] đang nghe [Song] trên [Device]...")

    try:
        while True:
            # Chọn ngẫu nhiên bài hát
            track = random.choice(tracks)
            
            # Tạo sự kiện
            event = generate_event(track)
            
            # Gửi vào Kafka
            producer.send(TOPIC_NAME, value=event)
            
            # In log đẹp
            print(f"🎧 {event['event_time_str']} | User {event['user_id']} ({event['location']}) "
                  f"nghe '{event['track_name']}' trên {event['device']} "
                  f"[{event['status']}]")
            
            # Random tốc độ bắn tin
            time.sleep(random.uniform(0.1, 1.0))

    except KeyboardInterrupt:
        producer.close()
        print("\n🛑 Đã dừng.")

if __name__ == "__main__":
    main()