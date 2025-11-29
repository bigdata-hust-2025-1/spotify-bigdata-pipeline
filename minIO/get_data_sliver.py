from minio_client import get_minio_client
import io, pyarrow.parquet as pq
from pathlib import Path

client = get_minio_client()
bucket = "spotify-silver"
obj = "tracks/ingest_date=2025-11-29/data.parquet"

resp = client.get_object(bucket, obj)
buf = io.BytesIO(resp.read())
resp.close(); resp.release_conn()

table = pq.read_table(buf)
df = table.to_pandas()

# Xuất ra CSV trong cùng thư mục minIO
out_path = Path(__file__).resolve().parent / "tracks_full.csv"
df.to_csv(str(out_path), index=False)
print(f"Saved CSV -> {out_path}")