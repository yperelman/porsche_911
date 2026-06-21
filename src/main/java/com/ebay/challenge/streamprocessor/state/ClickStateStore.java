package com.ebay.challenge.streamprocessor.state;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-user store of ad click events, keyed by (eventTime, clickId) to dedup
 * duplicate clicks and allow ties at the same event time. Thread-safe by
 * composition: ConcurrentHashMap for the per-user index, ConcurrentSkipListMap
 * for the in-window-sorted clicks. No global lock.
 */
@Component
public class ClickStateStore {

    private final Duration attributionWindow;

    @Autowired
    public ClickStateStore(@Value("${attribution.window-minutes:30}") long attributionWindowMinutes) {
        this.attributionWindow = Duration.ofMinutes(attributionWindowMinutes);
    }

    //used for correct dedup and deterministic results in case of 2 very close clicks
    record ClickKey(Instant eventTime, String clickId) implements Comparable<ClickKey> {
        @Override
        public int compareTo(ClickKey other) {
            int byTime = eventTime.compareTo(other.eventTime);
            return byTime != 0 ? byTime : clickId.compareTo(other.clickId);
        }
    }

    private final Map<String, ConcurrentSkipListMap<ClickKey, AdClickEvent>> clicksByUser = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicLong> clickCountByPartition = new ConcurrentHashMap<>();

    public boolean addClick(AdClickEvent click) {
        AdClickEvent previous = clicksByUser
                .computeIfAbsent(click.getUserId(), u -> new ConcurrentSkipListMap<>())
                .putIfAbsent(new ClickKey(click.getEventTime(), click.getClickId()), click);
        boolean added = previous == null;
        if (added) {
            clickCountByPartition
                    .computeIfAbsent(click.getPartition(), p -> new AtomicLong())
                    .incrementAndGet();
        }
        return added;
    }

    /**
     * Returns whether {@code click} is a duplicate logical click delivered from a
     * different Kafka source offset than the retained state entry. A retry of the
     * retained source offset returns false and must remain uncommitted until eviction;
     * a duplicate from another offset returns true and may become done once its
     * idempotent correction has been flushed.
     */
    public boolean isDuplicateFromDifferentSourceOffset(AdClickEvent click) {
        ConcurrentSkipListMap<ClickKey, AdClickEvent> userClicks = clicksByUser.get(click.getUserId());
        if (userClicks == null) {
            return false;
        }
        AdClickEvent retained = userClicks.get(new ClickKey(click.getEventTime(), click.getClickId()));
        return retained != null
                && (retained.getPartition() != click.getPartition()
                        || retained.getOffset() != click.getOffset());
    }

    public AdClickEvent findAttributableClick(String userId, Instant pageViewTime) {
        ConcurrentSkipListMap<ClickKey, AdClickEvent> userClicks = clicksByUser.get(userId);
        if (userClicks == null) {
            return null;
        }
        // lowerEntry((pageViewTime + 1ns, "")) returns the greatest composite key strictly less
        // than (pageViewTime+1ns, ""). Every real (eventTime, clickId) with eventTime ≤ pageViewTime
        // qualifies; every entry with eventTime > pageViewTime does not. This is the sentinel-free
        // way to include clicks whose eventTime exactly equals pageViewTime (a low/empty clickId
        // on the search key would otherwise exclude them via the composite comparator).
        Map.Entry<ClickKey, AdClickEvent> latest = userClicks.lowerEntry(new ClickKey(pageViewTime.plusNanos(1), ""));
        if (latest == null) {
            return null;
        }
        Instant windowStart = pageViewTime.minus(attributionWindow);
        if (latest.getKey().eventTime().isBefore(windowStart)) {
            return null;
        }
        return latest.getValue();
    }

    /**
     * Evict clicks older than {@code cutoffTime}, but only for users whose clicks
     * live on {@code partition}. A user's clicks are always on a single Kafka
     * partition (because Kafka partitions by {@code user_id}), so checking one
     * click suffices to know the user's partition.
     *
     * <p>This is the call the JoinEngine should use: it ensures that a fast
     * partition's joined-watermark advance does not silently evict clicks for
     * users on slower partitions and break their attribution.
     */
    public List<AdClickEvent> evictOldClicks(int partition, Instant cutoffTime) {
        List<AdClickEvent> evicted = new ArrayList<>();
        ClickKey boundary = new ClickKey(cutoffTime, "");
        var userIter = clicksByUser.entrySet().iterator();
        while (userIter.hasNext()) {
            var userEntry = userIter.next();
            var userClicks = userEntry.getValue();
            // A user's clicks are all on the same partition (Kafka hashes by user_id),
            // so a single sample tells us which partition this user belongs to.
            var firstEntry = userClicks.firstEntry();
            if (firstEntry == null || firstEntry.getValue().getPartition() != partition) {
                continue;
            }
            var head = userClicks.headMap(boundary, false);
            List<AdClickEvent> removed = new ArrayList<>(head.values());
            evicted.addAll(removed);
            decrementPartitionCounts(removed);
            head.clear();
            if (userClicks.isEmpty()) {
                userIter.remove();
            }
        }
        return evicted;
    }

    public long getTotalClickCount() {
        return clickCountByPartition.values().stream().mapToLong(AtomicLong::get).sum();
    }

    public long getClickCount(int partition) {
        AtomicLong count = clickCountByPartition.get(partition);
        return count == null ? 0 : count.get();
    }

    private void decrementPartitionCounts(List<AdClickEvent> removed) {
        for (AdClickEvent click : removed) {
            AtomicLong count = clickCountByPartition.get(click.getPartition());
            if (count != null) {
                count.decrementAndGet();
            }
        }
    }
}
