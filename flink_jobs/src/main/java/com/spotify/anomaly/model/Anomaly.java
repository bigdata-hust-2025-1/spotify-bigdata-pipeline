package com.spotify.anomaly.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

/**
 * A detected anomaly: a user who skipped more than the configured threshold of
 * tracks within one window. Emitted as JSON to the anomaly Kafka topic.
 */
public class Anomaly implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonProperty("user_id")
    public String userId;

    @JsonProperty("skip_count")
    public long skipCount;

    /** Window bounds in epoch milliseconds. */
    @JsonProperty("window_start")
    public long windowStart;

    @JsonProperty("window_end")
    public long windowEnd;

    @JsonProperty("anomaly_type")
    public String anomalyType;

    public Anomaly() {
    }

    public Anomaly(String userId, long skipCount, long windowStart, long windowEnd) {
        this.userId = userId;
        this.skipCount = skipCount;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.anomalyType = "SKIP_BURST";
    }

    public String toJson() throws JsonProcessingException {
        return MAPPER.writeValueAsString(this);
    }
}
