package com.ebay.challenge.streamprocessor.observability;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bounded, thread-safe ring of recent event "journeys" backing the dashboard's
 * live event stream, attribution timeline, and event inspector.
 *
 * <p>Each input event (page view or ad click) gets one {@link EventTrace}, keyed
 * by its stable id ({@code event_id} for page views, {@code click_id} for clicks).
 * Hot-path components append human-readable steps as the event moves through the
 * engine. The structure is capped at {@code dashboard.event-log.capacity}; the
 * oldest trace is evicted when a new one pushes past the cap.
 *
 * <p>All operations are O(1) and lock-free on the common path so the hot listener
 * threads never block on the dashboard. Disable entirely with
 * {@code dashboard.event-log.enabled=false} to remove all overhead.
 */
@Component
public class RecentEventLog {

    public enum EventType { PAGE_VIEW, AD_CLICK }

    public record Step(Instant at, String label) {}

    /** A single event and the ordered steps it has been through. */
    public static final class EventTrace {
        private final EventType type;
        private final String id;
        private final String userId;
        private final Instant eventTime;
        private final int partition;
        private final long offset;
        private final Instant firstSeen;
        private final List<Step> steps = Collections.synchronizedList(new ArrayList<>());

        EventTrace(EventType type, String id, String userId, Instant eventTime,
                   int partition, long offset, Instant firstSeen) {
            this.type = type;
            this.id = id;
            this.userId = userId;
            this.eventTime = eventTime;
            this.partition = partition;
            this.offset = offset;
            this.firstSeen = firstSeen;
        }

        public EventType getType() { return type; }
        public String getId() { return id; }
        public String getUserId() { return userId; }
        public Instant getEventTime() { return eventTime; }
        public int getPartition() { return partition; }
        public long getOffset() { return offset; }
        public Instant getFirstSeen() { return firstSeen; }

        /** Defensive copy so callers (e.g. the JSON serializer) see a stable snapshot. */
        public List<Step> getSteps() {
            synchronized (steps) {
                return List.copyOf(steps);
            }
        }

        String latestLabel() {
            synchronized (steps) {
                return steps.isEmpty() ? null : steps.get(steps.size() - 1).label();
            }
        }
    }

    private final boolean enabled;
    private final int capacity;
    private final Map<String, EventTrace> tracesById = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<String> order = new ConcurrentLinkedDeque<>();
    private final AtomicInteger size = new AtomicInteger();

    // Dedicated index of events that reached an attribution. Attribution lands long
    // after consumption (only once the watermark passes), so by then the live ring
    // above has usually evicted the trace. This separately-bounded index lets the
    // attribution timeline survive that consumption churn.
    private final Map<String, EventTrace> attributedById = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<String> attributedOrder = new ConcurrentLinkedDeque<>();
    private final AtomicInteger attributedSize = new AtomicInteger();

    public RecentEventLog(
            @Value("${dashboard.event-log.enabled:true}") boolean enabled,
            @Value("${dashboard.event-log.capacity:500}") int capacity) {
        this.enabled = enabled;
        this.capacity = capacity;
    }

    /**
     * Records a newly consumed event with an initial "consumed" step. A repeated id
     * (e.g. a duplicate event) annotates the existing trace rather than replacing it;
     * a fresh trace is only created on first sight.
     */
    public void recordConsumed(EventType type, String id, String userId, Instant eventTime,
                               int partition, long offset) {
        if (!enabled || id == null) {
            return;
        }
        EventTrace trace = new EventTrace(type, id, userId, eventTime, partition, offset, Instant.now());
        EventTrace previous = tracesById.putIfAbsent(id, trace);
        if (previous != null) {
            // Duplicate id within the window — keep the original trace, just annotate it.
            previous.steps.add(new Step(Instant.now(), "consumed (duplicate id)"));
            return;
        }
        trace.steps.add(new Step(Instant.now(), "consumed"));
        pushBounded(tracesById, order, size, id);
    }

    /** Appends a step to an existing trace. No-op if disabled or the id is unknown. */
    public void addStep(String id, String label) {
        if (!enabled || id == null) {
            return;
        }
        EventTrace trace = tracesById.get(id);
        if (trace != null) {
            trace.steps.add(new Step(Instant.now(), label));
        }
    }

    /**
     * Records the terminal attribution of a page view. Appends the attribution and
     * terminal steps to the live trace if it still exists, otherwise reconstructs a
     * minimal trace (the live one was already evicted under load). Page views that
     * actually matched a click are indexed in a dedicated, separately-bounded
     * attribution ring so {@link #recentAttributions(int)} and {@link #byId(String)}
     * keep surfacing them regardless of consumption churn.
     */
    public void recordAttribution(EventType type, String id, String userId, Instant eventTime,
                                  int partition, long offset, String attributionLabel, String terminalLabel) {
        if (!enabled || id == null) {
            return;
        }
        EventTrace trace = tracesById.get(id);
        if (trace == null) {
            trace = new EventTrace(type, id, userId, eventTime, partition, offset, Instant.now());
        }
        trace.steps.add(new Step(Instant.now(), attributionLabel));
        trace.steps.add(new Step(Instant.now(), terminalLabel));
        if (attributionLabel.startsWith("attributed") && attributedById.putIfAbsent(id, trace) == null) {
            pushBounded(attributedById, attributedOrder, attributedSize, id);
        }
    }

    /** Append an id to a bounded ring, evicting the oldest entry once over capacity. */
    private void pushBounded(Map<String, EventTrace> byId, ConcurrentLinkedDeque<String> deque,
                             AtomicInteger counter, String id) {
        deque.addLast(id);
        if (counter.incrementAndGet() > capacity) {
            String oldest = deque.pollFirst();
            if (oldest != null) {
                byId.remove(oldest);
                counter.decrementAndGet();
            }
        }
    }

    /** Newest-first snapshot of up to {@code limit} traces. */
    public List<EventTrace> recent(int limit) {
        List<EventTrace> out = new ArrayList<>();
        var it = order.descendingIterator();
        while (it.hasNext() && out.size() < limit) {
            EventTrace t = tracesById.get(it.next());
            if (t != null) {
                out.add(t);
            }
        }
        return out;
    }

    /**
     * Newest-first snapshot of up to {@code limit} attributed page views, read from
     * the dedicated attribution index so it is unaffected by live-ring eviction.
     */
    public List<EventTrace> recentAttributions(int limit) {
        List<EventTrace> out = new ArrayList<>();
        var it = attributedOrder.descendingIterator();
        while (it.hasNext() && out.size() < limit) {
            EventTrace t = attributedById.get(it.next());
            if (t != null) {
                out.add(t);
            }
        }
        return out;
    }

    public EventTrace byId(String id) {
        EventTrace t = tracesById.get(id);
        return t != null ? t : attributedById.get(id);
    }

    public int size() {
        return size.get();
    }

    /** Latest step label, for compact summaries. */
    public static String latestLabelOf(EventTrace trace) {
        return trace.latestLabel();
    }
}
