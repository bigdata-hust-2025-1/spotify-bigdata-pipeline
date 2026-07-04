# Flink — real-time anomaly detection

`AnomalyDetectionJob` consumes the playback-events topic from Kafka and (for now)
filters candidate anomalies to `stdout`. This module builds a shaded uber-jar
that is submitted to a Flink cluster.

## Status

- **This PR (PR-05):** migrates the source off the removed `FlinkKafkaConsumer`
  API to the Flink 1.17 `KafkaSource`, so the module **compiles and packages**
  again. The detection logic is unchanged (a stateless string filter + `print()`
  sink) — genuinely **stateful, windowed** anomaly detection and a Kafka anomaly
  sink land in **PR-10**.

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

`TOPIC_PLAYBACK` uses the same name and default as the Python `common.config`
so the whole platform reads/writes one unified topic (finding A2). Java cannot
import the Python module, hence the shared **env-var convention** rather than a
shared constant.

## Compatibility note

The `pom.xml` pins `flink.version = 1.17.1` and targets Java 11 (Flink 1.17's
supported runtime). Newer JDKs can *build* the jar, but submit it to a
Flink 1.17 cluster running on Java 8/11.
