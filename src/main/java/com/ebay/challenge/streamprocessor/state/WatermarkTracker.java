package com.ebay.challenge.streamprocessor.state;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks watermarks per (partition, topic) pair. A watermark represents the
 * event-time frontier up to which we believe we have seen all events for that
 * stream slice. Watermarks advance monotonically — an out-of-order older event
 * never moves the watermark backward.
 *
 * <p><strong>Idle-source detection (Tier 2):</strong> Spring Kafka partition-idle
 * events mark a source idle only when the listener is unpaused and caught up to
 * the broker end offset. The joined-watermark calculation excludes those idle
 * topics, so a genuinely silent topic does not freeze state eviction.
 */
@Slf4j
@Component
public class WatermarkTracker {

    /**
     * Hard cap on configurable lateness. Per the README "Stream Join +
     * Out-of-Order Handling" section: "Configurable allowed lateness (max 15
     * minutes)". Larger values are rejected at construction time.
     */
    public static final int MAX_ALLOWED_LATENESS_MINUTES = 15;

    private final Duration allowedLateness;
    /** The two streams that participate in the join, by Kafka topic name. */
    private final String clicksTopic;
    private final String pageViewsTopic;
    private final Clock clock;
    private final Map<String, Map<Integer, Instant>> topicPartitionWatermarks = new ConcurrentHashMap<>();
    private final Set<TopicPartitionKey> idleSet = ConcurrentHashMap.newKeySet();
    private final Map<Integer, Instant> joinedWatermarks = new ConcurrentHashMap<>();
    // Wall-clock instant of each partition's most recent joined-watermark advance,
    // for the dashboard's processing-staleness metric (how long since we made progress).
    private final Map<Integer, Instant> lastAdvanceWallClock = new ConcurrentHashMap<>();

    private record TopicPartitionKey(int partition, String topic) {}

    @Autowired
    public WatermarkTracker(
            @Value("${watermark.allowed-lateness-minutes:15}") int allowedLatenessMinutes,
            @Value("${kafka.topics.ad-clicks:ad_clicks}") String clicksTopic,
            @Value("${kafka.topics.page-views:page_views}") String pageViewsTopic,
            Clock clock) {
        if (allowedLatenessMinutes < 0 || allowedLatenessMinutes > MAX_ALLOWED_LATENESS_MINUTES) {
            throw new IllegalArgumentException(
                    "watermark.allowed-lateness-minutes must be in [0, "
                            + MAX_ALLOWED_LATENESS_MINUTES + "] (README spec); got "
                            + allowedLatenessMinutes);
        }
        this.allowedLateness = Duration.ofMinutes(allowedLatenessMinutes);
        this.clicksTopic = clicksTopic;
        this.pageViewsTopic = pageViewsTopic;
        this.clock = clock;
    }

    /** Test convenience: system UTC clock. */
    public WatermarkTracker(int allowedLatenessMinutes, String clicksTopic, String pageViewsTopic) {
        this(allowedLatenessMinutes, clicksTopic, pageViewsTopic, Clock.systemUTC());
    }

    public void updateWatermark(int partition, String topic, Instant eventTime) {
        topicPartitionWatermarks
                .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                .merge(partition, eventTime, (existing, candidate) ->
                        candidate.isAfter(existing) ? candidate : existing);
        // A real event resumes an explicitly idle source.
        TopicPartitionKey key = new TopicPartitionKey(partition, topic);
        if (idleSet.remove(key)) {
            log.info("Idle source resumed: partition={}, topic={}", partition, topic);
        }
    }

    public Instant getWatermark(int partition, String topic) {
        Map<Integer, Instant> topicMap = topicPartitionWatermarks.get(topic);
        if (topicMap == null) {
            return Instant.MIN;
        }
        return topicMap.getOrDefault(partition, Instant.MIN);
    }

    /**
     * Returns the join watermark for a partition: the minimum of the per-topic
     * watermarks for that partition, excluding idle topics. If a non-idle topic
     * has never produced an event on this partition, returns {@link Optional#empty()}.
     */
    public Optional<Instant> getJoinedWatermark(int partition) {
        boolean clicksIdle = isIdle(partition, clicksTopic);
        boolean pageViewsIdle = isIdle(partition, pageViewsTopic);
        Instant clicksWm = getWatermark(partition, clicksTopic);
        Instant pageViewsWm = getWatermark(partition, pageViewsTopic);

        // A non-idle topic that hasn't produced yet (watermark still MIN) blocks the
        // joined frontier — we can't claim completeness for a stream we haven't heard from.
        if ((!clicksIdle && clicksWm.equals(Instant.MIN))
                || (!pageViewsIdle && pageViewsWm.equals(Instant.MIN))) {
            return Optional.empty();
        }
        // Every topic idle → keep the last established joined frontier, if any.
        if (clicksIdle && pageViewsIdle) {
            return Optional.ofNullable(joinedWatermarks.get(partition));
        }
        // Exactly one idle → the other carries the frontier alone.
        if (clicksIdle) {
            return advanceJoinedWatermark(partition, pageViewsWm);
        }
        if (pageViewsIdle) {
            return advanceJoinedWatermark(partition, clicksWm);
        }
        // Both contributing → the join frontier is the slower (min) of the two.
        return advanceJoinedWatermark(partition, clicksWm.isBefore(pageViewsWm) ? clicksWm : pageViewsWm);
    }

    /**
     * A {@code (partition, topic)} is idle only after a Kafka idle event proves
     * that the source is caught up and unpaused.
     */
    private boolean isIdle(int partition, String topic) {
        return idleSet.contains(new TopicPartitionKey(partition, topic));
    }

    private Optional<Instant> advanceJoinedWatermark(int partition, Instant newFrontier) {
        Instant prior = joinedWatermarks.get(partition);
        Instant updated = joinedWatermarks.merge(partition, newFrontier,
                (existing, next) -> next.isAfter(existing) ? next : existing);
        // Record wall-clock progress only when the frontier strictly advances, so a
        // dashboard read (which also calls this) does not reset the staleness clock.
        if (prior == null || updated.isAfter(prior)) {
            lastAdvanceWallClock.put(partition, clock.instant());
        }
        return Optional.of(updated);
    }

    /**
     * Wall-clock instant of this partition's most recent joined-watermark advance,
     * or empty if it has never advanced. Drives the dashboard's processing-staleness
     * (time since the pipeline last made progress), which — unlike event-time-minus-
     * wall-clock — stays meaningful for historical/backfill data.
     */
    public Optional<Instant> lastProgressAt(int partition) {
        return Optional.ofNullable(lastAdvanceWallClock.get(partition));
    }

    /**
     * Mark a source idle only after Kafka proves it is caught up and the listener
     * event says it is not paused. Wall-clock silence alone is insufficient: a
     * paused/backlogged partition still has pending records and must constrain the join.
     */
    public void markIdleIfCaughtUp(int partition, String topic, boolean partitionDrained, boolean paused) {
        if (!partitionDrained || paused) {
            idleSet.remove(new TopicPartitionKey(partition, topic));
            return;
        }
        TopicPartitionKey key = new TopicPartitionKey(partition, topic);
        if (idleSet.add(key)) {
            log.warn("Idle source detected: partition={}, topic={}, watermark={}",
                    partition, topic, getWatermark(partition, topic));
        }
    }

    /**
     * An event is too late if its event time is at or before
     * (joined watermark - allowedLateness). Until the joined watermark has
     * been initialized, no event is considered too late.
     */
    public boolean isTooLate(int partition, Instant eventTime) {
        Optional<Instant> joined = getJoinedWatermark(partition);
if (joined.isEmpty()) {
            return false;
        }
        return !eventTime.isAfter(joined.get().minus(allowedLateness));
    }

    public Duration getAllowedLateness() {
        return allowedLateness;
    }
}
