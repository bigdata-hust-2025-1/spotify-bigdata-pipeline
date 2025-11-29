from minio import Minio
from minio_client import get_minio_client # CONFIG
import pandas as pd
import io


SILVER_BUCKET = "spotify-silver"
GOLD_BUCKET = "spotify-gold"

# Tùy chỉnh nếu muốn lấy ngày cố định
INGEST_DATE = "2025-11-29"

def ensure_bucket(client: Minio, bucket_name: str):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    else:
        print(f"Bucket exists: {bucket_name}")


def read_parquet_from_minio(client: Minio, bucket: str, obj_path: str) -> pd.DataFrame:
    print(f"  - Đọc Silver: s3://{bucket}/{obj_path}")
    resp = client.get_object(bucket, obj_path)
    data_bytes = resp.read()
    resp.close()
    resp.release_conn()

    df = pd.read_parquet(io.BytesIO(data_bytes))
    print(f"    + {len(df)} dòng, {len(df.columns)} cột")
    return df


def write_parquet_to_minio(client: Minio, df: pd.DataFrame, bucket: str, obj_path: str):
    print(f"  - Ghi Gold: s3://{bucket}/{obj_path}")
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    length = buf.getbuffer().nbytes

    client.put_object(
        bucket,
        obj_path,
        data=buf,
        length=length,
        content_type="application/octet-stream",
    )
    print("    + DONE")


# ========== GOLD: ARTISTS STATS ==========
def build_gold_artists_stats(client: Minio):
    print("\n=== GOLD: artists_stats ===")

    # 1. Đọc Silver
    tracks_obj = f"tracks/ingest_date={INGEST_DATE}/data.parquet"
    artists_obj = f"artists/ingest_date={INGEST_DATE}/data.parquet"

    df_tracks = read_parquet_from_minio(client, SILVER_BUCKET, tracks_obj)
    df_artists = read_parquet_from_minio(client, SILVER_BUCKET, artists_obj)

    # 2. Từ tracks_silver: explode artist_ids -> mỗi dòng = 1 (track, artist)
    if "artist_ids" not in df_tracks.columns:
        raise ValueError("tracks Silver không có cột 'artist_ids'")

    df_tracks = df_tracks.dropna(subset=["artist_ids"])
    df_tracks["artist_id_list"] = df_tracks["artist_ids"].str.split(",")

    df_exploded = df_tracks.explode("artist_id_list").rename(
        columns={"artist_id_list": "artist_id"}
    )

    # 3. Group theo artist_id: tính thống kê
    #    - số bài (track_count)
    #    - độ phổ biến trung bình (avg_track_popularity)
    #    - độ phổ biến max (max_track_popularity)
    grp = df_exploded.groupby("artist_id", as_index=False).agg(
        track_count=("id", "count"),
        avg_track_popularity=("popularity", "mean"),
        max_track_popularity=("popularity", "max"),
    )

    # 4. Join với artists_silver để lấy name, genres, followers_total
    df_artists_small = df_artists[
        ["artist_id", "name", "genres", "followers_total", "popularity"]
    ].rename(columns={"name": "artist_name", "popularity": "artist_popularity"})

    df_gold = grp.merge(df_artists_small, on="artist_id", how="left")

    # 5. Sắp xếp theo track_count / avg_popularity cho dễ xem
    df_gold = df_gold.sort_values(
        by=["track_count", "avg_track_popularity"], ascending=[False, False]
    )

    gold_obj = f"artists_stats/ingest_date={INGEST_DATE}/data.parquet"
    write_parquet_to_minio(client, df_gold, GOLD_BUCKET, gold_obj)


# ========== GOLD: ALBUMS STATS ==========
def build_gold_albums_stats(client: Minio):
    print("\n=== GOLD: albums_stats ===")

    # 1. Đọc Silver
    tracks_obj = f"tracks/ingest_date={INGEST_DATE}/data.parquet"
    albums_obj = f"albums/ingest_date={INGEST_DATE}/data.parquet"

    df_tracks = read_parquet_from_minio(client, SILVER_BUCKET, tracks_obj)
    df_albums = read_parquet_from_minio(client, SILVER_BUCKET, albums_obj)

    # 2. Group theo album_id trong tracks:
    #    - số bài thực tế (track_count)
    #    - độ phổ biến trung bình
    #    - tổng thời lượng
    if "album_id" not in df_tracks.columns:
        raise ValueError("tracks Silver không có cột 'album_id'")

    grp = df_tracks.groupby("album_id", as_index=False).agg(
        track_count_in_tracks=("id", "count"),
        avg_track_popularity=("popularity", "mean"),
        total_duration_sec=("duration_sec", "sum"),
    )

    # 3. Join với albums_silver để lấy metadata album
    df_albums_small = df_albums[
        [
            "album_id",
            "name",
            "album_type",
            "total_tracks",
            "release_date",
            "artist_ids",
            "genres",
            "label",
            "popularity",
        ]
    ].rename(columns={"name": "album_name", "popularity": "album_popularity"})

    df_gold = grp.merge(df_albums_small, on="album_id", how="left")

    # 4. Sắp xếp: album nhiều track & phổ biến hơn đứng trên
    df_gold = df_gold.sort_values(
        by=["track_count_in_tracks", "avg_track_popularity"],
        ascending=[False, False],
    )

    gold_obj = f"albums_stats/ingest_date={INGEST_DATE}/data.parquet"
    write_parquet_to_minio(client, df_gold, GOLD_BUCKET, gold_obj)


# ========== MAIN ==========
if __name__ == "__main__":
    client = get_minio_client()
    ensure_bucket(client, GOLD_BUCKET)

    # Xây 2 bảng Gold chính
    try:
        build_gold_artists_stats(client)
    except Exception as e:
        print(f"!!! Lỗi build_gold_artists_stats: {e}")

    try:
        build_gold_albums_stats(client)
    except Exception as e:
        print(f"!!! Lỗi build_gold_albums_stats: {e}")

    print("\nDONE: Silver -> Gold")
