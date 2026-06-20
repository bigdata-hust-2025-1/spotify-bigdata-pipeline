package com.spotify.anomaly;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class AnomalyDetectionJob {
    public static void main(String[] args) throws Exception {
        // Cấu hình môi trường execution Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Kết nối đến Kafka
        String kafkaBroker = System.getenv().getOrDefault("KAFKA_BROKER", "kafka.bigdata:9092");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBroker);
        properties.setProperty("group.id", "anomaly-detection-group");
        
        // Tiêu thụ luồng dữ liệu hành vi người dùng từ topic Kafka
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "spotify-user-events", new SimpleStringSchema(), properties);
                
        DataStream<String> stream = env.addSource(consumer);
        
        // Phát hiện bất thường realtime: Cảnh báo người dùng skip bài hát liên tục hoặc tương tác bất thường
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
