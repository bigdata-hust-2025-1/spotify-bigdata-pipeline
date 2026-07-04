# upload_to_bronze.py
import os
import sys
from datetime import date

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from minio_client import get_minio_client  # noqa: E402
from common.config import DATA_DIR  # noqa: E402

BRONZE_BUCKET = "spotify-bronze"

# Thư mục chứa các file json nguồn; mặc định <repo>/data, override bằng DATA_DIR.
LOCAL_DATA_DIR = DATA_DIR

def ensure_bucket(client, bucket_name: str):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    else:
        print(f"Bucket exists: {bucket_name}")

def main():
    client = get_minio_client()
    ensure_bucket(client, BRONZE_BUCKET)

    ingest_date = date.today().isoformat()   # real time "2025-11-29"
    # ingest_date = "2025-11-29"              cố định 1 ngày

    # Mapping: local file -> (domain, remote file name)
    files_config = {
        # albums
        "albums.json": ("albums", "albums.json"),
        "albums_processing_metadata.json": ("albums", "albums_processing_metadata.json"),

        # artists
        "artists.json": ("artists", "artists.json"),

        # owners
        "owners.json": ("owners", "owners.json"),

        # playlists
        "playlists.json": ("playlists", "playlists.json"),
        "playlists_processing_metadata.json": ("playlists", "playlists_processing_metadata.json"),

        # tracks
        "tracks.json": ("tracks", "tracks.json"),
        "tracks_processing_metadata.json": ("tracks", "tracks_processing_metadata.json"),
    }

    for local_name, (domain, remote_name) in files_config.items():
        local_path = os.path.join(LOCAL_DATA_DIR, local_name)

        if not os.path.exists(local_path):
            print(f"[SKIP] Không tìm thấy file local: {local_path}")
            continue

        #  khớp với Bronze -> Silver:
        #   domain/ingest_date=YYYY-MM-DD/filename.json
        object_name = f"{domain}/ingest_date={ingest_date}/{remote_name}"

        print(f"[UPLOAD] {local_path} -> s3://{BRONZE_BUCKET}/{object_name}")
        client.fput_object(BRONZE_BUCKET, object_name, local_path)

    print("\n=== DONE UPLOAD BRONZE ===")
    print("Vào MinIO để kiểm tra cấu trúc:")
    print("  - spotify-bronze/")
    print("      albums/ingest_date=YYYY-MM-DD/...")
    print("      artists/ingest_date=YYYY-MM-DD/...")
    print("      playlists/ingest_date=YYYY-MM-DD/...")
    print("      tracks/ingest_date=YYYY-MM-DD/...")
    print("      owners/ingest_date=YYYY-MM-DD/...")


if __name__ == "__main__":
    main()
