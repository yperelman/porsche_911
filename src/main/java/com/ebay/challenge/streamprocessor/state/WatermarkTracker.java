package com.ebay.challenge.streamprocessor.state;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Tracks watermarks per partition to handle out-of-order events.
 *
 * Watermark represents the point in event-time up to which we believe we have seen all events.
 * Events arriving with event_time < watermark - allowedLateness are considered too late.
 *
 * TODO: Implement per-partition watermark tracking
 */
@Slf4j
@Component
public class WatermarkTracker {

    private final Duration allowedLateness;

    // TODO: Add data structure to track watermarks per partition
    // Hint: Use ConcurrentHashMap<Integer, Instant> for thread-safe partition watermarks

    public WatermarkTracker(@Value("${watermark.allowed-lateness-minutes:2}") int allowedLatenessMinutes) {
        this.allowedLateness = Duration.ofMinutes(allowedLatenessMinutes);
        log.info("Initialized WatermarkTracker with allowed lateness: {} minutes", allowedLatenessMinutes);
    }

    /**
     * Update watermark for a partition based on observed event time.
     * Watermark advances monotonically (never goes backward).
     *
     * TODO: Implement watermark advancement
     * - Update partition watermark if event time is later than current watermark
     * - Ensure watermark never goes backward
     * - Handle concurrent updates
     *
     * @param partition the partition ID
     * @param eventTime the event timestamp
     */
    public void updateWatermark(int partition, Instant eventTime) {
        // TODO: Implement this method
        log.debug("Updating watermark for partition {} with event time {}", partition, eventTime);
    }

    /**
     * Get current watermark for a partition.
     *
     * TODO: Implement watermark retrieval
     * - Return current watermark for the partition
     * - Return Instant.MIN if partition has no watermark yet
     *
     * @param partition the partition ID
     * @return the current watermark, or Instant.MIN if not yet initialized
     */
    public Instant getWatermark(int partition) {
        // TODO: Implement this method
        return Instant.MIN;
    }

    /**
     * Check if an event is too late (beyond allowed lateness).
     *
     * TODO: Implement late event detection
     * - Calculate cutoff time as: watermark - allowedLateness
     * - Return true if event is before cutoff time
     * - Handle case when watermark is not yet initialized
     *
     * @param partition the partition ID
     * @param eventTime the event timestamp
     * @return true if the event is too late and should be dropped
     */
    public boolean isTooLate(int partition, Instant eventTime) {
        // TODO: Implement this method
        return false;
    }

    /**
     * Get the allowed lateness duration.
     *
     * @return the allowed lateness duration
     */
    public Duration getAllowedLateness() {
        return allowedLateness;
    }
}
