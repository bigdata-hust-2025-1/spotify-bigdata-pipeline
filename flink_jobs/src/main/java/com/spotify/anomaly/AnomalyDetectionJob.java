package com.spotify.anomaly;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AnomalyDetectionJob {
    public static void main(String[] args) throws Exception {
        // Cấu hình môi trường execution Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kết nối đến Kafka. Topic khớp với luồng playback thống nhất (finding A2);
        // dùng biến môi trường TOPIC_PLAYBACK giống common.config bên Python.
        String kafkaBroker = System.getenv().getOrDefault("KAFKA_BROKER", "kafka.bigdata:9092");
        String topic = System.getenv().getOrDefault("TOPIC_PLAYBACK", "spotify_playback_events");

        // Flink 1.17 đã bỏ FlinkKafkaConsumer -> dùng KafkaSource API mới.
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBroker)
                .setTopics(topic)
                .setGroupId("anomaly-detection-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Nguồn không cần watermark ở bước này (xử lý có trạng thái sẽ làm ở PR-10).
        DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

        // Phát hiện bất thường realtime (giữ nguyên filter cũ — đây là bước
        // compile-only; logic cửa sổ có trạng thái sẽ được thêm ở PR-10).
        DataStream<String> anomalies = stream.filter(event -> {
            // Logic bắt các điều kiện lạ (Ví dụ: "skip" bài hát mà thời lượng nghe < 2 giây)
            return event.contains("\"action\":\"skip\"") && event.contains("\"duration\":2");
        });

        // In cảnh báo ra log hoặc có thể đưa ra một Topic Kafka khác để thông báo
        anomalies.print();

        // Khởi chạy Job
        env.execute("Spotify Real-time Anomaly Detection with Flink");
    }
}
