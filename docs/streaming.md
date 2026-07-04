# Real-time streaming — offsets, checkpoints & delivery semantics

Two Structured Streaming jobs consume the playback-events topic
(`common.config.TOPIC_PLAYBACK`, produced by `produce_to_kafka.py`):

| Job | Sink | Purpose |
| :--- | :--- | :--- |
| `spark_jobs/stream/stream_processing.py` | Elasticsearch index `realtime_events` | Real-time dashboard / Kibana |
| `spark_jobs/stream/stream_to_cassandra.py` | Cassandra `spotify_ks.user_plays` | Low-latency per-user serving |

Both parse the same payload via the shared `spark_jobs/stream/event_schema.py`.

## Checkpoints & offset recovery

Every query writes its checkpoint under a **durable object-store root**,
`common.config.CHECKPOINT_ROOT` (default `s3a://spotify-checkpoints`), namespaced
per query by `checkpoint_location(job_name)`:

- `s3a://spotify-checkpoints/stream_processing_es`
- `s3a://spotify-checkpoints/stream_to_cassandra`

Checkpoints **must never** live under `/tmp` — that is local to the driver pod
and is lost on restart, which would force a full reprocess or silently drop
data. With a durable checkpoint, a restarted query resumes from the last
**committed Kafka offsets**.

**Offset-reset policy.** `startingOffsets` is `latest`, which only applies on a
*cold* start (no existing checkpoint). Once a checkpoint exists it is
authoritative and `startingOffsets` is ignored — restarts never re-read from the
beginning.

## Delivery semantics & de-duplication

Kafka + checkpointing gives **at-least-once** source delivery. On top of that:

- Both jobs set an **event-time watermark** (`event_time`, 10 min) and call
  `dropDuplicatesWithinWatermark(["event_id"])`, removing duplicate events that
  arrive within the watermark (e.g. a producer retry or a post-restart replay).
- **Elasticsearch** writes use `es.mapping.id = event_id`, so a replayed event
  upserts the same document instead of creating a duplicate.
- **Cassandra** keys `user_plays` on `((user_id), event_time, event_id)`, so a
  replayed event upserts the same row.

Together these make each sink **effectively idempotent** on `event_id`.

## Cassandra schema

`stream_to_cassandra.py` no longer writes raw Kafka bytes. It decodes the JSON
payload and projects exactly `event_schema.USER_PLAYS_COLUMNS`, which is kept in
lock-step with `cassandra/user_plays.cql` (enforced by
`tests/test_event_schema.py`). Create the table before running the job:

```bash
cqlsh -f cassandra/user_plays.cql
```

## RPO / RTO

- **RPO (data-loss window):** ~0 for committed offsets — after a driver crash
  the query resumes from the last committed batch; at most the in-flight
  micro-batch is reprocessed, and de-dup/upserts absorb the overlap.
- **RTO (recovery time):** the time to reschedule the driver pod and restore
  state from the checkpoint (seconds-to-minutes on Kubernetes).
- Losing the checkpoint root (object store) is the real disaster case; it is
  itself protected by the object store's durability/replication.

## Running locally

```bash
# 1. create the Cassandra table
cqlsh -f cassandra/user_plays.cql
# 2. point the jobs at a durable checkpoint bucket (any S3A/MinIO path)
export CHECKPOINT_ROOT=s3a://spotify-checkpoints
# 3. submit (packages elided) — e.g.
spark-submit spark_jobs/stream/stream_to_cassandra.py
spark-submit spark_jobs/stream/stream_processing.py
```

To verify restart semantics: produce N events, stop the driver mid-stream,
restart it, and confirm the sink row/doc counts show no gap and no duplicates.
