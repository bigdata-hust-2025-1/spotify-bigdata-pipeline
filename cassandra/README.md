# cassandra/

Cassandra schema for the low-latency serving copy of playback events.

| File | Role |
| :--- | :--- |
| `user_plays.cql` | Keyspace + `user_plays` table DDL — the sink for the streaming job (`spark_jobs/stream/stream_to_cassandra.py`). |

Apply with `cqlsh -f cassandra/user_plays.cql`. The streaming writer's output
columns must match this table (typed `from_json`, append mode). The table is a
**rebuildable serving copy**, not a system of record — see
`docs/DR_AND_SCALING.md`.
