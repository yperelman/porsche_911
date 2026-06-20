package com.ebay.challenge.streamprocessor.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * Represents a page view event from the stream.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@SuperBuilder
public class PageViewEvent extends StreamEvent {

    @JsonProperty("url")
    private String url;

    @JsonProperty("event_id")
    private String eventId;
}
