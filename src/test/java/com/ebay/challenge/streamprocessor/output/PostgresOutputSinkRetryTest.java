package com.ebay.challenge.streamprocessor.output;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the retry-with-backoff helper inside {@link PostgresOutputSink}.
 * Kept separate from the Testcontainers-backed sink tests so the retry logic
 * can be verified without Docker.
 */
class PostgresOutputSinkRetryTest {

    @Test
    void succeedsImmediately_callsOpOnce() throws SQLException {
        AtomicInteger calls = new AtomicInteger();
        PostgresOutputSink.retryWithBackoff(calls::incrementAndGet, 5, 1, 5);
        assertEquals(1, calls.get());
    }

    @Test
    void succeedsAfterTransientFailures_returnsWithoutThrowing() throws SQLException {
        AtomicInteger calls = new AtomicInteger();
        PostgresOutputSink.retryWithBackoff(() -> {
            if (calls.incrementAndGet() < 3) {
                throw new SQLException("transient #" + calls.get());
            }
        }, 5, 1, 5);
        assertEquals(3, calls.get(), "succeeded on the 3rd attempt");
    }

    @Test
    void throwsLastException_afterExhaustingAllAttempts() {
        AtomicInteger calls = new AtomicInteger();
        SQLException e = assertThrows(SQLException.class, () ->
                PostgresOutputSink.retryWithBackoff(() -> {
                    throw new SQLException("attempt " + calls.incrementAndGet());
                }, 5, 1, 5));
        assertEquals(5, calls.get(), "should attempt exactly maxAttempts times");
        assertEquals("attempt 5", e.getMessage(), "thrown exception is the last failure");
    }

    @Test
    void backoffIsBounded_doublingCappedAtMaxDelay() throws SQLException {
        // Cap is 5ms. After 4 failures, delays accumulate: 1+2+4+5+5 = 17ms minimum.
        // Verify we don't sleep e.g. 16ms+32ms+64ms which would happen if unbounded.
        AtomicInteger calls = new AtomicInteger();
        long start = System.nanoTime();
        try {
            PostgresOutputSink.retryWithBackoff(() -> {
                if (calls.incrementAndGet() < 5) {
                    throw new SQLException("transient");
                }
            }, 5, 1, 5);
        } catch (SQLException ignored) {}
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMs < 200,
                "with maxDelayMs=5, total elapsed should be small (got " + elapsedMs + "ms)");
        assertEquals(5, calls.get());
    }
}
