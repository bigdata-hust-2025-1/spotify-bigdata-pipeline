# Flink — real-time anomaly detection

`AnomalyDetectionJob` consumes the playback-events topic from Kafka, detects
per-user skip bursts over event-time windows, and emits anomalies to a Kafka
topic. This module builds a shaded uber-jar submitted to a Flink cluster.

## Status

- **PR-10 (current):** genuinely **stateful, windowed** detection. Events are
  parsed to `PlaybackEvent`, assigned event-time watermarks, `keyBy(user_id)`,
  and counted over a tumbling window; a user with more than
  `ANOMALY_SKIP_THRESHOLD` `skipped` plays in the window emits an `Anomaly`
  (JSON) to `TOPIC_ANOMALY`. Checkpointing is enabled so the keyed window state
  is durable and recovers on restart.
- **PR-05 (prior):** migrated the source off the removed `FlinkKafkaConsumer`
  API to the Flink 1.17 `KafkaSource` (compile-only).

## Pipeline

```
KafkaSource(TOPIC_PLAYBACK, String)
  -> flatMap: JSON -> PlaybackEvent (drop malformed)
  -> assignTimestampsAndWatermarks (event time = producer `timestamp`)
  -> keyBy(user_id)
  -> window(TumblingEventTime, ANOMALY_WINDOW_SECONDS)
  -> process: count `skipped`; if > ANOMALY_SKIP_THRESHOLD emit Anomaly
  -> KafkaSink(TOPIC_ANOMALY, JSON)
```

## Build

```bash
cd flink_jobs
mvn -q -B clean package
# -> target/flink-anomaly-detection-1.0-SNAPSHOT.jar  (shaded uber-jar)
```

The build bundles the Kafka connector, merges its `META-INF/services` SPI, and
sets `Main-Class: com.spotify.anomaly.AnomalyDetectionJob`. Flink core
(`flink-java`, `flink-streaming-java`) is `provided` — supplied by the cluster.

## Run

```bash
flink run -c com.spotify.anomaly.AnomalyDetectionJob \
  target/flink-anomaly-detection-1.0-SNAPSHOT.jar
```

## Configuration

| Env var | Default | Purpose |
| :--- | :--- | :--- |
| `KAFKA_BROKER` | `kafka.bigdata:9092` | Kafka bootstrap servers |
| `TOPIC_PLAYBACK` | `spotify_playback_events` | Source topic — matches the producer and `common/config.py` |
| `TOPIC_ANOMALY` | `spotify_anomaly_events` | Sink topic for detected anomalies (matches `common.config.TOPIC_ANOMALY`) |
| `ANOMALY_SKIP_THRESHOLD` | `5` | Skips per user per window that trigger an anomaly |
| `ANOMALY_WINDOW_SECONDS` | `60` | Tumbling event-time window length |
| `FLINK_CHECKPOINTS` | *(cluster default)* | Optional checkpoint storage dir (e.g. `s3a://spotify-checkpoints/flink`) |

`TOPIC_PLAYBACK` / `TOPIC_ANOMALY` use the same names and defaults as the Python `common.config`
so the whole platform reads/writes one unified topic (finding A2). Java cannot
import the Python module, hence the shared **env-var convention** rather than a
shared constant.

## Compatibility note

The `pom.xml` pins `flink.version = 1.17.1` and targets Java 11 (Flink 1.17's
supported runtime). Newer JDKs can *build* the jar, but submit it to a
Flink 1.17 cluster running on Java 8/11.
