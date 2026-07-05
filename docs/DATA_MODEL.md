# Dimensional model — star schema + SCD2 (PR-09)

This document describes the conformed **star schema** built in the Iceberg
`gold_star` namespace: three SCD2 dimensions and one event-grain fact.

## Why

Silver mirrors the source JSON (e.g. `artist_ids` as a delimited string) and the
legacy Gold `*_stats` tables flatten-then-reaggregate it with no conformed
dimensions, surrogate keys, or history (finding B2). PR-09 adds a proper
dimensional model **additively**, in a new namespace, leaving the legacy
`*_stats` tables (PR-08) untouched until a later cutover.

> **Additive-first note.** The roadmap suggested changing the Silver
> `artist_ids` column from a delimited string to an `array<string>`. That mutates
> the *shared* Silver schema and would break legacy consumers
> (`minIO/silver_to_gold.py`) and PR-08's Gold, violating the "main stays green"
> principle. Instead the array split happens **inside the model** (`dim_track`),
> so the acceptance criterion "no delimited-string id columns remain in the
> model" holds while Silver is unchanged. The Silver-side change is a follow-up
> cutover.

## ERD (grain + keys)

```
        dim_artist                 dim_album                  dim_track
        ----------                 ---------                  ---------
        surrogate_key (PK)         surrogate_key (PK)         surrogate_key (PK)
        artist_id (BK)             album_id (BK)              track_id (BK)
        name, genres,              name, album_type,          name, album_id,
        followers_total,           total_tracks,              artist_ids[],
        popularity                 release_date, label,       duration_ms,
        [SCD2 cols]                popularity  [SCD2 cols]    duration_category,
             |                          |                     explicit, popularity,
             |                          |                     release_date [SCD2]
             |                          |                          |
             +-----------+   +----------+        +----------------+
                         |   |                   |
                     fact_playback  (grain: one playback event)
                     -------------
                     event_id (degenerate PK, MERGE key)
                     user_id, event_time
                     track_sk  -> dim_track.surrogate_key
                     artist_sk -> dim_artist.surrogate_key
                     album_sk  -> dim_album.surrogate_key
                     listen_duration_ms, track_popularity, is_skipped (measures)
                     status, location, device (context)
                     partitioned by days(event_time)
```

- **PK** = surrogate key (deterministic hash, see below). **BK** = business
  (natural) key from the source. Facts reference dimensions by **surrogate key**.

## Surrogate keys & change detection (pure, unit-tested)

`common.modeling` generates the SQL — no JVM needed to test it:

- **`attr_hash`** = `sha2(concat_ws('||', coalesce(cast(col as string),'__NULL__') …), 256)`
  over the tracked attributes. Null-safe and order-sensitive: an unchanged row
  hashes identically (idempotent load); any tracked change flips it.
- **`surrogate_key`** = `sha2(concat_ws('||', <business key>, cast(valid_from as string)), 256)`.
  Deterministic and unique **per version** (business key + `valid_from`), so a
  retried load reproduces the same key.

## SCD2 load (idempotent three-step)

For each dimension, per run:

1. **Stage** the current snapshot from Silver with `valid_from` (run date),
   `valid_to = NULL`, `is_current = true`, `attr_hash`, `surrogate_key`.
2. **Close** — `MERGE` the staging onto the open (`is_current=true`) row on the
   business key; `WHEN MATCHED AND t.attr_hash <> s.attr_hash` set
   `is_current=false`, `valid_to = s.valid_from`.
3. **Insert** — `INSERT … SELECT … LEFT ANTI JOIN` the open rows on the business
   key: brand-new keys and just-closed (changed) keys have no open row, so a new
   version is inserted; unchanged keys are skipped.

**Idempotency:** re-running the same input changes nothing — step 2 finds equal
hashes (no close), step 3's anti-join excludes still-open keys (no insert). A
changed attribute produces exactly one closed row + one new version (`is_current`
flips) — the SCD2 acceptance test.

## fact_playback

- **Grain:** one row per playback event (`event_id`).
- **Source:** `spotify_ks.user_plays` in **Cassandra** — the durable event store
  landed by the streaming sink (PR-04). This reads the *serving* store because
  raw events are not yet landed to the lake; a Bronze/Silver `events` dataset
  would be the more standard source and is a noted follow-up. `FACT_SOURCE`
  selects the source so that swap is config-only.
- **Load:** left-join each event to the *current* version of `dim_track` /
  `dim_artist` / `dim_album` for the surrogate keys, then `MERGE` on `event_id`
  into `gold_star.fact_playback` (idempotent), partitioned by `days(event_time)`.
- **Referential integrity:** `unresolved_fk_count_sql` counts rows where any
  surrogate key is `NULL`; a healthy load returns 0 (every fact FK resolves).

## Verifying (needs Spark 3.5 + MinIO + Cassandra)

```bash
export SILVER_FORMAT=iceberg
export MINIO_ACCESS_KEY=… MINIO_SECRET_KEY=…
spark-submit spark_jobs/batch/build_dimensions.py   # SCD2 dims
spark-submit spark_jobs/batch/build_facts.py        # fact_playback from Cassandra
```

```sql
-- SCD2 history exists and exactly one version is current per key
SELECT artist_id, count(*) versions, sum(cast(is_current as int)) current_cnt
FROM lakehouse.gold_star.dim_artist GROUP BY artist_id HAVING current_cnt <> 1;   -- empty
-- Referential integrity: every fact FK resolves
SELECT count(*) FROM lakehouse.gold_star.fact_playback
WHERE track_sk IS NULL OR artist_sk IS NULL OR album_sk IS NULL;                  -- 0
```

## Configuration

| Variable | Default | Notes |
| :--- | :--- | :--- |
| `GOLD_STAR_NAMESPACE` | `gold_star` | Iceberg namespace for the star model. |
| `SILVER_FORMAT` | `parquet` | How dimensions read Silver (iceberg/parquet). |
| `FACT_SOURCE` | `cassandra` | Playback-event source for the fact. |
| `CASSANDRA_SPARK_CONNECTOR` | `com.datastax.spark:spark-cassandra-connector_2.12:3.5.0` | Connector coordinate. |

## Known limitations / follow-ups

- **Silver arrays:** the Silver `artist_ids` string→array cutover is deferred
  (see additive-first note); the split lives in `dim_track` for now.
- **Fact source:** reads Cassandra (serving store); landing events to
  Bronze/Silver first is the more standard pattern.
- **Late-arriving dimensions:** a fact event whose track/artist/album is not yet
  in the dimension resolves to a NULL surrogate key (surfaced by the integrity
  check); inferred-member handling is a future refinement.
