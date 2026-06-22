package com.ebay.challenge.streamprocessor.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Bug 3: the {@code event_time} pattern {@code yyyy-MM-dd'T'HH:mm:ss} rejects
 * ordinary ISO-8601 — a trailing {@code Z} and fractional seconds both throw
 * {@code DateTimeParseException}, turning standards-compliant producers' events
 * into poison-pill records. These events must parse.
 */
class EventDeserializationTest {

    private static ObjectMapper mapper() {
        ObjectMapper m = new ObjectMapper();
        m.registerModule(new JavaTimeModule());
        return m;
    }

    @Test
    void adClick_parsesIso8601WithZoneZ() throws Exception {
        String json = "{\"user_id\":\"u\",\"event_time\":\"2024-01-01T12:05:00Z\","
                + "\"campaign_id\":\"c\",\"click_id\":\"k\"}";
        AdClickEvent c = mapper().readValue(json, AdClickEvent.class);
        assertEquals(Instant.parse("2024-01-01T12:05:00Z"), c.getEventTime());
    }

    @Test
    void pageView_parsesFractionalSeconds_truncatedToSeconds() throws Exception {
        // Fractional input is accepted (no longer a poison pill) and floored to whole
        // seconds — the output contract is seconds-precision.
        String json = "{\"user_id\":\"u\",\"event_time\":\"2024-01-01T12:05:00.123Z\","
                + "\"url\":\"http://x\",\"event_id\":\"e\"}";
        PageViewEvent p = mapper().readValue(json, PageViewEvent.class);
        assertEquals(Instant.parse("2024-01-01T12:05:00Z"), p.getEventTime());
    }
}
