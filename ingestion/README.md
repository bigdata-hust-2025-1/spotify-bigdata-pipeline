# ingestion/

The producer/consumer edge of the pipeline: crawl Spotify, publish to Kafka, and
land raw events as Bronze on the object store.

| Path | Role |
| :--- | :--- |
| `producer/crawl_spotify.py` | Calls the Spotify Web API and produces playback/catalog events to Kafka. |
| `consumer/kafka_to_minio.py`, `consumer_to_minio.py` | Consume Kafka and write raw JSON to the Bronze bucket. |
| `producer/.env.example`, `consumer/.env.example` | Credential templates — copy to `.env` (git-ignored) and fill in. **Never commit real secrets** (see `docs/SECRETS.md`). |
| `Dockerfile`, `*.requirements.txt` | Container images and pinned deps for the ingest services. |

Topic names come from `common.config` (`TOPIC_PLAYBACK`, …) — do not hardcode.
