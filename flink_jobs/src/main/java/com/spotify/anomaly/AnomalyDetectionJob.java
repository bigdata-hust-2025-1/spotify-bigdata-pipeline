package com.spotify.anomaly;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.anomaly.model.Anomaly;
import com.spotify.anomaly.model.PlaybackEvent;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Genuinely stateful anomaly detection (PR-10).
 *
 * <p>Replaces the previous stateless string filter (which also matched fields
 * that do not exist in the real event schema) with a real event-time pipeline:
 * parse JSON to {@link PlaybackEvent}, assign event-time watermarks, key by
 * user, and over a tumbling window count "skipped" plays. A user who skips more
 * than a threshold within the window emits an {@link Anomaly} to the anomaly
 * Kafka topic. Checkpointing is enabled so the keyed window state is durable and
 * recoverable.
 */
public class AnomalyDetectionJob {

    public static void main(String[] args) throws Exception {
        final String kafkaBroker =
                System.getenv().getOrDefault("KAFKA_BROKER", "kafka.bigdata:9092");
        final String inputTopic =
                System.getenv().getOrDefault("TOPIC_PLAYBACK", "spotify_playback_events");
        final String anomalyTopic =
                System.getenv().getOrDefault("TOPIC_ANOMALY", "spotify_anomaly_events");
        final int skipThreshold =
                Integer.parseInt(System.getenv().getOrDefault("ANOMALY_SKIP_THRESHOLD", "5"));
        final int windowSeconds =
                Integer.parseInt(System.getenv().getOrDefault("ANOMALY_WINDOW_SECONDS", "60"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Durable keyed-window state: checkpoint so a restart resumes in-flight
        // windows. Storage defaults to Flink's config; override via env for an
        // object-store checkpoint dir (e.g. s3a://spotify-checkpoints/flink).
        env.enableCheckpointing(60_000L);
        String checkpointDir = System.getenv("FLINK_CHECKPOINTS");
        if (checkpointDir != null && !checkpointDir.isEmpty()) {
            env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
        }

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBroker)
                .setTopics(inputTopic)
                .setGroupId("anomaly-detection-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Parse to typed events, then assign event-time watermarks from the
        // producer timestamp (bounded out-of-orderness tolerates late events).
        DataStream<PlaybackEvent> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                .flatMap(new ParseEvent())
                .name("parse-json")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PlaybackEvent>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(10))
                                .withTimestampAssigner((e, ts) -> e.eventTimeMillis()));

        // Keyed, event-time tumbling window counting skips per user.
        DataStream<Anomaly> anomalies = events
                .keyBy(e -> e.userId)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSeconds)))
                .process(new SkipBurstDetector(skipThreshold))
                .name("skip-burst-detector");

        KafkaSink<String> anomalySink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBroker)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(anomalyTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        anomalies.flatMap(new AnomalyToJson()).name("anomaly-json").sinkTo(anomalySink);

        env.execute("Spotify Real-time Anomaly Detection with Flink");
    }

    /** Parse a raw JSON line to a {@link PlaybackEvent}, dropping malformed records. */
    public static class ParseEvent implements FlatMapFunction<String, PlaybackEvent> {
        private static final long serialVersionUID = 1L;
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public void flatMap(String raw, Collector<PlaybackEvent> out) {
            try {
                PlaybackEvent event = MAPPER.readValue(raw, PlaybackEvent.class);
                if (event.userId != null && event.status != null) {
                    out.collect(event);
                }
            } catch (Exception e) {
                // Skip unparseable / partial records rather than failing the job.
            }
        }
    }

    /** Serialize an {@link Anomaly} to JSON, dropping any that fail to serialize. */
    public static class AnomalyToJson implements FlatMapFunction<Anomaly, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(Anomaly anomaly, Collector<String> out) {
            try {
                out.collect(anomaly.toJson());
            } catch (Exception e) {
                // Unreachable in practice; guard keeps the sink resilient.
            }
        }
    }

    /**
     * Per-user, per-window skip counter. Emits an {@link Anomaly} only when the
     * number of skipped plays in the window exceeds the threshold.
     */
    public static class SkipBurstDetector
            extends ProcessWindowFunction<PlaybackEvent, Anomaly, String, TimeWindow> {
        private static final long serialVersionUID = 1L;
        private final int threshold;

        public SkipBurstDetector(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public void process(String userId, Context ctx,
                            Iterable<PlaybackEvent> events, Collector<Anomaly> out) {
            long skips = 0;
            for (PlaybackEvent event : events) {
                if (event.isSkip()) {
                    skips++;
                }
            }
            if (skips > threshold) {
                out.collect(new Anomaly(
                        userId, skips, ctx.window().getStart(), ctx.window().getEnd()));
            }
        }
    }
}
