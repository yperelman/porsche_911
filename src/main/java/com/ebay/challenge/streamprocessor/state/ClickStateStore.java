package com.ebay.challenge.streamprocessor.state;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Stores ad click events partitioned by user_id for efficient windowed joins.
 *
 * Thread-safe implementation with per-user locking for fine-grained concurrency.
 * Implements state eviction to prevent unbounded memory growth.
 *
 * TODO: Implement thread-safe state storage and retrieval
 */
@Slf4j
@Component
public class ClickStateStore {

    // Attribution window: clicks within last 30 minutes can be attributed
    private static final Duration ATTRIBUTION_WINDOW = Duration.ofMinutes(30);

    // TODO: Add data structures to store clicks per user
    // Hint: Consider /home/yuval/Downloads/porsche_911using ConcurrentHashMap and TreeSet for thread-safe, sorted storage

    /**
     * Add a click event to the state store.
     *
     * TODO: Implement thread-safe click storage
     * - Use locks for thread safety
     * - Store clicks sorted by event time (most recent first)
     * - Handle concurrent access properly
     *
     * @param click the ad click event
     */
    public void addClick(AdClickEvent click) {
        // TODO: Implement this method
        log.debug("Adding click {} for user {}", click.getClickId(), click.getUserId());
    }

    /**
     * Find the most recent click for a user within the attribution window.
     *
     * TODO: Implement attribution logic
     * - Search for clicks in window: [pageViewTime - 30 minutes, pageViewTime]
     * - Return the most recent click within the window
     * - Return null if no click found
     *
     * @param userId the user ID
     * @param pageViewTime the page view event time
     * @return the most recent click within 30 minutes before the page view, or null if none found
     */
    public AdClickEvent findAttributableClick(String userId, Instant pageViewTime) {
        // TODO: Implement this method
        log.debug("Finding attributable click for user {} at time {}", userId, pageViewTime);
        return null;
    }

    /**
     * Evict old clicks that are beyond the retention window.
     * Prevents unbounded memory growth.
     *
     * TODO: Implement state eviction
     * - Remove clicks older than the cutoff time
     * - Clean up empty user entries
     * - Return count of evicted clicks
     *
     * @param cutoffTime clicks older than this time should be evicted
     * @return number of clicks evicted
     */
    public int evictOldClicks(Instant cutoffTime) {
        // TODO: Implement this method
        log.debug("Evicting clicks older than {}", cutoffTime);
        return 0;
    }

    /**
     * Get the total number of clicks currently in state.
     *
     * @return total click count across all users
     */
    public long getTotalClickCount() {
        // TODO: Implement this method
        return 0;
    }
}
