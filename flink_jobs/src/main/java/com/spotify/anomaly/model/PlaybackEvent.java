package com.spotify.anomaly.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * A single playback event, matching the producer payload in
 * {@code spark_jobs/stream/produce_to_kafka.generate_event} and the shared
 * schema in {@code spark_jobs/stream/event_schema.py}.
 *
 * <p>Only the fields the anomaly job needs are modelled; unknown JSON properties
 * are ignored so the POJO stays decoupled from the full event shape. Public
 * fields + a no-arg constructor keep it a Flink-friendly POJO (efficient
 * serialization, usable as keyed-stream state).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PlaybackEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("event_id")
    public String eventId;

    @JsonProperty("user_id")
    public String userId;

    @JsonProperty("track_id")
    public String trackId;

    /** Epoch seconds (producer uses {@code time.time()}). */
    @JsonProperty("timestamp")
    public double timestamp;

    /** {@code "completed"} or {@code "skipped"}. */
    @JsonProperty("status")
    public String status;

    public PlaybackEvent() {
    }

    /** Event time in milliseconds, for Flink event-time watermarks/windows. */
    public long eventTimeMillis() {
        return (long) (timestamp * 1000.0);
    }

    public boolean isSkip() {
        return "skipped".equals(status);
    }
}
