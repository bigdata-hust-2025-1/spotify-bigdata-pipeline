from minio import Minio
from minio_client import get_minio_client
import pandas as pd
import io

BRONZE_BUCKET = "spotify-bronze"
SILVER_BUCKET = "spotify-silver"

# SỬA CHO ĐÚNG NGÀY KHI DÙNG LẠI KHI UPLOAD
INGEST_DATE = "2025-11-29"

def ensure_bucket(client: Minio, bucket_name: str):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    else:
        print(f"Bucket exists: {bucket_name}")


def read_json_array(client: Minio, bucket: str, obj_path: str) -> pd.DataFrame:
    print(f"  - Đọc Bronze: s3://{bucket}/{obj_path}")
    resp = client.get_object(bucket, obj_path)
    data_bytes = resp.read()
    resp.close()
    resp.release_conn()

    df = pd.read_json(io.BytesIO(data_bytes))  # dữ liệu là 1 mảng JSON []
    print(f"    + {len(df)} dòng, {len(df.columns)} cột ban đầu")
    return df


def write_parquet(client: Minio, df: pd.DataFrame, bucket: str, obj_path: str):
    print(f"  - Ghi Silver: s3://{bucket}/{obj_path}")
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


# ========== TRACKS ==========
def process_tracks(client: Minio):
    print("\n=== DATASET: tracks ===")
    bronze_obj = f"tracks/ingest_date={INGEST_DATE}/tracks.json"
    silver_obj = f"tracks/ingest_date={INGEST_DATE}/data.parquet"

    df = read_json_array(client, BRONZE_BUCKET, bronze_obj)

    # album_id
    def extract_album_id(album):
        if isinstance(album, dict):
            return album.get("id")
        return None

    df["album_id"] = df["album"].apply(extract_album_id)

    # artist_ids: list -> "id1,id2,..."
    def extract_artist_ids(artists):
        if isinstance(artists, list):
            ids = [a.get("id") for a in artists if isinstance(a, dict) and "id" in a]
            return ",".join(ids)
        return None

    df["artist_ids"] = df["artists"].apply(extract_artist_ids)

    # duration_sec
    if "duration_ms" in df.columns:
        df["duration_sec"] = df["duration_ms"] / 1000.0

    # giữ cột quan trọng
    keep_cols = [
        "id",
        "name",
        "album_id",
        "artist_ids",
        "disc_number",
        "track_number",
        "duration_ms",
        "duration_sec",
        "explicit",
        "popularity",
        "release_date",
        "is_local",
        "type",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df_clean = df[keep_cols].copy()

    print(f"    + Sau xử lý: {len(df_clean)} dòng, {len(df_clean.columns)} cột")
    write_parquet(client, df_clean, SILVER_BUCKET, silver_obj)


# ========== ALBUMS ==========
def process_albums(client: Minio):
    print("\n=== DATASET: albums ===")
    bronze_obj = f"albums/ingest_date={INGEST_DATE}/albums.json"
    silver_obj = f"albums/ingest_date={INGEST_DATE}/data.parquet"

    df = read_json_array(client, BRONZE_BUCKET, bronze_obj)

    # artist_ids
    def extract_artist_ids(artists):
        if isinstance(artists, list):
            ids = [a.get("id") for a in artists if isinstance(a, dict) and "id" in a]
            return ",".join(ids)
        return None

    df["artist_ids"] = df["artists"].apply(extract_artist_ids)

    # genres
    def extract_genres(genres):
        if isinstance(genres, list):
            return ",".join([str(g) for g in genres])
        return None

    if "genres" in df.columns:
        df["genres_str"] = df["genres"].apply(extract_genres)
    else:
        df["genres_str"] = None

    # copyrights C / P
    def extract_copyrights(copyrights, ctype):
        if isinstance(copyrights, list):
            for c in copyrights:
                if isinstance(c, dict) and c.get("type") == ctype:
                    return c.get("text")
        return None

    df["copyright_text_C"] = df["copyrights"].apply(
        lambda x: extract_copyrights(x, "C")
    )
    df["copyright_text_P"] = df["copyrights"].apply(
        lambda x: extract_copyrights(x, "P")
    )

    keep_cols = [
        "id",
        "name",
        "album_type",
        "total_tracks",
        "release_date",
        "type",
        "artist_ids",
        "genres_str",
        "label",
        "popularity",
        "copyright_text_C",
        "copyright_text_P",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df_clean = df[keep_cols].copy()
    df_clean = df_clean.rename(columns={"id": "album_id", "genres_str": "genres"})

    print(f"    + Sau xử lý: {len(df_clean)} dòng, {len(df_clean.columns)} cột")
    write_parquet(client, df_clean, SILVER_BUCKET, silver_obj)


# ========== ARTISTS ==========
def process_artists(client: Minio):
    print("\n=== DATASET: artists ===")
    bronze_obj = f"artists/ingest_date={INGEST_DATE}/artists.json"
    silver_obj = f"artists/ingest_date={INGEST_DATE}/data.parquet"

    df = read_json_array(client, BRONZE_BUCKET, bronze_obj)

    # followers_total
    def extract_followers_total(followers):
        if isinstance(followers, dict):
            return followers.get("total")
        return None

    df["followers_total"] = df["followers"].apply(extract_followers_total)

    # genres
    def extract_genres(genres):
        if isinstance(genres, list):
            return ",".join([str(g) for g in genres])
        return None

    if "genres" in df.columns:
        df["genres_str"] = df["genres"].apply(extract_genres)
    else:
        df["genres_str"] = None

    keep_cols = [
        "id",
        "name",
        "genres_str",
        "followers_total",
        "popularity",
        "type",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df_clean = df[keep_cols].copy()
    df_clean = df_clean.rename(columns={"id": "artist_id", "genres_str": "genres"})

    print(f"    + Sau xử lý: {len(df_clean)} dòng, {len(df_clean.columns)} cột")
    write_parquet(client, df_clean, SILVER_BUCKET, silver_obj)


# ========== OWNERS ==========
def process_owners(client: Minio):
    print("\n=== DATASET: owners ===")
    bronze_obj = f"owners/ingest_date={INGEST_DATE}/owners.json"
    silver_obj = f"owners/ingest_date={INGEST_DATE}/data.parquet"

    df = read_json_array(client, BRONZE_BUCKET, bronze_obj)

    keep_cols = ["id", "display_name", "type"]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df_clean = df[keep_cols].copy()
    df_clean = df_clean.rename(columns={"id": "owner_id"})

    print(f"    + Sau xử lý: {len(df_clean)} dòng, {len(df_clean.columns)} cột")
    write_parquet(client, df_clean, SILVER_BUCKET, silver_obj)


# ========== PLAYLISTS ==========
def process_playlists(client: Minio):
    print("\n=== DATASET: playlists ===")
    bronze_obj = f"playlists/ingest_date={INGEST_DATE}/playlists.json"
    silver_obj = f"playlists/ingest_date={INGEST_DATE}/data.parquet"

    df = read_json_array(client, BRONZE_BUCKET, bronze_obj)

    # followers_total
    def extract_followers_total(followers):
        if isinstance(followers, dict):
            return followers.get("total")
        return None

    df["followers_total"] = df["followers"].apply(extract_followers_total)

    # owner flatten
    def extract_owner_id(owner):
        if isinstance(owner, dict):
            return owner.get("id")
        return None

    def extract_owner_name(owner):
        if isinstance(owner, dict):
            return owner.get("display_name")
        return None

    def extract_owner_type(owner):
        if isinstance(owner, dict):
            return owner.get("type")
        return None

    df["owner_id"] = df["owner"].apply(extract_owner_id)
    df["owner_display_name"] = df["owner"].apply(extract_owner_name)
    df["owner_type"] = df["owner"].apply(extract_owner_type)

    keep_cols = [
        "id",
        "name",
        "description",
        "collaborative",
        "public",
        "followers_total",
        "owner_id",
        "owner_display_name",
        "owner_type",
        "primary_color",
        "snapshot_id",
        "type",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df_clean = df[keep_cols].copy()
    df_clean = df_clean.rename(columns={"id": "playlist_id"})

    print(f"    + Sau xử lý: {len(df_clean)} dòng, {len(df_clean.columns)} cột")
    write_parquet(client, df_clean, SILVER_BUCKET, silver_obj)


# ========== MAIN ==========
if __name__ == "__main__":
    client = get_minio_client()
    ensure_bucket(client, SILVER_BUCKET)

    # chạy lần lượt tất cả dataset
    for fn in [
        process_tracks,
        process_albums,
        process_artists,
        process_owners,
        process_playlists,
    ]:
        try:
            fn(client)
        except Exception as e:
            print(f"!!! Lỗi ở {fn.__name__}: {e}")

    print("\nDONE: Bronze -> Silver cho toàn bộ datasets ✅")
