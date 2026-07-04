-- Cassandra schema for the real-time playback serving table.
-- Applied by ops before running spark_jobs/stream/stream_to_cassandra.py.
--
-- The column set here is the contract that the Spark writer targets; it is
-- kept in sync with spark_jobs/stream/event_schema.USER_PLAYS_COLUMNS and
-- verified by tests/test_event_schema.py.

-- Dev/single-node keyspace. In production use NetworkTopologyStrategy with a
-- replication_factor of 3 across the serving datacenter.
CREATE KEYSPACE IF NOT EXISTS spotify_ks
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Query pattern: "most recent plays for a user".
-- Partition by user_id; cluster by event_time (newest first) then event_id so
-- each event is uniquely addressable and re-delivered events upsert in place
-- (idempotent by primary key).
CREATE TABLE IF NOT EXISTS spotify_ks.user_plays (
    user_id            text,
    event_time         timestamp,
    event_id           text,
    track_id           text,
    track_name         text,
    artist_id          text,
    album_id           text,
    track_popularity   int,
    listen_duration_ms int,
    status             text,
    location           text,
    device             text,
    PRIMARY KEY ((user_id), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC);
