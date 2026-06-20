package com.ebay.challenge.streamprocessor.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.time.Instant;

/**
 * Fields shared by both input streams: the joining identity ({@code user_id}),
 * the event-time, and the Kafka source coordinates ({@code partition}/{@code offset})
 * tagged on by the consumer for offset tracking.
 */
@Data
@NoArgsConstructor
@SuperBuilder
public abstract class StreamEvent {

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("event_time")
    @JsonDeserialize(using = LenientInstantDeserializer.class)
    private Instant eventTime;

    // Metadata fields for processing (not part of the JSON payload).
    private transient int partition;
    private transient long offset;
}
