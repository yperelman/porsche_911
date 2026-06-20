package com.ebay.challenge.streamprocessor.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;

/**
 * Parses {@code event_time} from a range of ISO-8601 inputs so a standards-compliant
 * producer's event is never turned into a poison-pill record:
 * <ul>
 *   <li>ISO-8601 with {@code Z} / explicit offset / fractional seconds (via {@link Instant#parse});</li>
 *   <li>bare {@code yyyy-MM-dd'T'HH:mm:ss} with no zone — assumed UTC (the current producer format).</li>
 * </ul>
 *
 * <p>The result is truncated to whole seconds: the output contract is seconds-precision,
 * and all existing producers already emit whole seconds, so this is a no-op for them and
 * floors only newly-accepted fractional input.
 */
public class LenientInstantDeserializer extends JsonDeserializer<Instant> {

    @Override
    public Instant deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
        String s = p.getValueAsString();
        if (s == null || s.isBlank()) {
            return null;
        }
        s = s.trim();
        Instant parsed;
        try {
            parsed = Instant.parse(s);
        } catch (DateTimeParseException e) {
            parsed = LocalDateTime.parse(s).toInstant(ZoneOffset.UTC);
        }
        return parsed.truncatedTo(ChronoUnit.SECONDS);
    }
}
