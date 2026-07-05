import os
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.config import ES_NODES, ES_PORT, GOLD_BUCKET, get_ingest_date  # noqa: E402
from common.spark import build_spark, gold_table  # noqa: E402

GOLD_PATH = f"s3a://{GOLD_BUCKET}"
INGEST_DATE = get_ingest_date()

# Same feature flag as silver_to_gold_all: default parquet keeps `main`
# behaviour-preserving; `iceberg` reads the Gold Iceberg catalog tables.
GOLD_FORMAT = os.getenv("GOLD_FORMAT", "parquet").lower()

# Elasticsearch-Spark connector coordinate (env-overridable, aligned to Spark
# 3.x / Scala 2.12). Declared here so the job carries its own JVM dependency
# instead of relying on an external --packages flag.
ES_SPARK_RUNTIME = os.getenv(
    "ES_SPARK_RUNTIME", "org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4"
)

# Gold dataset -> ES index. Both artists and albums now sync (PR-08 completes
# the previously artists-only sync). Indices are typeless (no legacy `/doc`
# mapping type), consistent with the streaming ES sink fixed in PR-04.
SYNC_TARGETS = {
    "artists_stats": "batch_artists",
    "albums_stats": "batch_albums",
}


def read_gold(spark, dataset):
    if GOLD_FORMAT == "iceberg":
        return spark.table(gold_table(dataset))
    return spark.read.parquet(f"{GOLD_PATH}/{dataset}/ingest_date={INGEST_DATE}")


def sync_to_es(df, index):
    (
        df.write.format("org.elasticsearch.spark.sql")
        .option("es.nodes", ES_NODES)
        .option("es.port", ES_PORT)
        .option("es.resource", index)
        .option("es.nodes.wan.only", "true")
        .mode("overwrite")
        .save()
    )


def main():
    spark = build_spark(
        "Spotify_Gold_To_ES",
        iceberg=(GOLD_FORMAT == "iceberg"),
        extra_packages=[ES_SPARK_RUNTIME],
    )
    print("--> Syncing Gold data to Elasticsearch...")
    for dataset, index in SYNC_TARGETS.items():
        try:
            df = read_gold(spark, dataset)
            sync_to_es(df, index)
            print(f"✅ {dataset} synced to ES index '{index}'.")
        except Exception as e:
            print(f"❌ Error syncing {dataset} -> '{index}': {e}")
    spark.stop()


if __name__ == "__main__":
    main()
