import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
import time

st.set_page_config(page_title="Spotify Streaming", page_icon="🎵", layout="wide")

# --- KẾT NỐI MONGODB ---
@st.cache_resource
def init_mongo():
    return MongoClient("mongodb://localhost:27017/")

try:
    client = init_mongo()
    db = client["spotify_db"]
    coll = db["playback_events"]
except:
    st.error("❌ Lỗi kết nối MongoDB. Nhớ chạy port-forward!")
    st.stop()

st.title("🎵 Spotify Live Monitor")
st.caption("Dữ liệu cập nhật thời gian thực từ Kafka & Spark Streaming")

# --- LAYOUT CONTAINER ---
metrics_container = st.empty()
charts_container = st.empty()
logs_container = st.empty()

while True:
    # Lấy 500 bản ghi mới nhất
    cursor = coll.find().sort("_id", -1).limit(500)
    data = list(cursor)
    u_key = time.time()

    if data:
        df = pd.DataFrame(data)
        
        # --- 1. METRICS (CON SỐ QUAN TRỌNG) ---
        with metrics_container.container():
            total_events = coll.count_documents({})
            
            # Tính số người nghe trong cửa sổ hiện tại
            active_users = df['user_id'].nunique()
            
            # Tính % nghe hết bài (Completion Rate)
            if 'status' in df.columns:
                completed = df[df['status'] == 'completed'].shape[0]
                total = len(df)
                completion_rate = (completed / total) * 100 if total > 0 else 0
            else:
                completion_rate = 0

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Tổng lượt nghe (Total)", f"{total_events:,}")
            k2.metric("User đang Online", active_users, delta_color="normal")
            k3.metric("Tỷ lệ nghe hết bài", f"{completion_rate:.1f}%")
            k4.metric("Cập nhật lúc", time.strftime("%H:%M:%S"))
            
            st.markdown("---")

        # --- 2. BIỂU ĐỒ (DỄ HIỂU) ---
        with charts_container.container():
            col1, col2 = st.columns(2)

            # [CHART 1] Line Chart: Xu hướng lượng truy cập (Traffic Trend)
            # Gom nhóm theo giây để vẽ đường
            if 'event_time_str' in df.columns:
                # Lấy giờ:phút:giây
                df['time_sec'] = pd.to_datetime(df['event_time_str']).dt.strftime('%H:%M:%S')
                traffic_df = df.groupby('time_sec').size().reset_index(name='count')
                
                fig_line = px.line(traffic_df, x='time_sec', y='count', markers=True,
                                   title='📈 Xu hướng lượng người nghe (Traffic Real-time)',
                                   labels={'time_sec': 'Thời gian', 'count': 'Số lượt truy cập'})
                col1.plotly_chart(fig_line, use_container_width=True, key=f"line_{u_key}")

            # [CHART 2] Horizontal Bar: Top Bài Hát (Trending)
            if 'track_name' in df.columns:
                top_songs = df['track_name'].value_counts().head(7).reset_index()
                top_songs.columns = ['Song', 'Plays']
                
                fig_bar = px.bar(top_songs, x='Plays', y='Song', orientation='h',
                                 title='🔥 Top Bài Hát đang Hot',
                                 color='Plays', color_continuous_scale='Bluered')
                # Đảo ngược trục Y để bài top 1 nằm trên cùng
                fig_bar.update_layout(yaxis=dict(autorange="reversed"))
                col2.plotly_chart(fig_bar, use_container_width=True, key=f"bar_{u_key}")

            # [CHART 3] Donut: Phân bố thiết bị (Thay vì Pie chart cũ)
            col3, col4 = st.columns(2)
            if 'device' in df.columns:
                fig_donut = px.pie(df, names='device', title='📱 Người dùng đang nghe bằng gì?', 
                                   hole=0.5) # hole=0.5 tạo thành hình cái bánh Donut
                col3.plotly_chart(fig_donut, use_container_width=True, key=f"donut_{u_key}")
            
            # [CHART 4] Simple Bar: Tương tác (Skip vs Complete)
            if 'status' in df.columns:
                 fig_status = px.bar(df, x='status', color='status', 
                                     title='⏯️ Chất lượng tương tác',
                                     color_discrete_map={'completed': '#00CC96', 'skipped': '#EF553B'})
                 col4.plotly_chart(fig_status, use_container_width=True, key=f"status_{u_key}")

        # --- 3. LOG ---
        with logs_container.container():
            st.markdown("### 📝 Nhật ký sự kiện")
            st.dataframe(df[['event_time_str', 'user_id', 'track_name', 'device', 'status']].head(5), use_container_width=True)

    else:
        st.warning("⏳ Đang chờ dữ liệu... Hãy chạy script Producer!")
    
    time.sleep(1) # Refresh nhanh hơn (1s) để thấy Line Chart chạy