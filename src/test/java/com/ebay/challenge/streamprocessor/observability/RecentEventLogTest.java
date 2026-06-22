package com.ebay.challenge.streamprocessor.observability;

import com.ebay.challenge.streamprocessor.observability.RecentEventLog.EventTrace;
import com.ebay.challenge.streamprocessor.observability.RecentEventLog.EventType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecentEventLogTest {

    private static final Instant T = Instant.parse("2024-01-01T12:00:00Z");

    private static RecentEventLog log(int capacity) {
        return new RecentEventLog(true, capacity);
    }

    @Test
    void recordsConsumedStepAndIsRetrievableById() {
        RecentEventLog log = log(10);
        log.recordConsumed(EventType.PAGE_VIEW, "pv_1", "user_1", T, 0, 5);

        EventTrace trace = log.byId("pv_1");
        assertNotNull(trace);
        assertEquals("user_1", trace.getUserId());
        assertEquals(0, trace.getPartition());
        assertEquals(5, trace.getOffset());
        assertEquals(List.of("consumed"), trace.getSteps().stream().map(RecentEventLog.Step::label).toList());
    }

    @Test
    void addStepAppendsInOrder() {
        RecentEventLog log = log(10);
        log.recordConsumed(EventType.PAGE_VIEW, "pv_1", "user_1", T, 0, 0);
        log.addStep("pv_1", "buffered");
        log.addStep("pv_1", "attributed → campaign_A (click click_1)");
        log.addStep("pv_1", "emitted");

        List<String> labels = log.byId("pv_1").getSteps().stream().map(RecentEventLog.Step::label).toList();
        assertEquals(List.of("consumed", "buffered", "attributed → campaign_A (click click_1)", "emitted"), labels);
    }

    @Test
    void addStepOnUnknownIdIsSafeNoOp() {
        RecentEventLog log = log(10);
        log.addStep("does_not_exist", "emitted");
        assertNull(log.byId("does_not_exist"));
        assertEquals(0, log.size());
    }

    @Test
    void evictsOldestPastCapacity() {
        RecentEventLog log = log(3);
        for (int i = 0; i < 5; i++) {
            log.recordConsumed(EventType.AD_CLICK, "c_" + i, "user", T, 0, i);
        }
        assertEquals(3, log.size());
        assertNull(log.byId("c_0"), "oldest should be evicted");
        assertNull(log.byId("c_1"), "second oldest should be evicted");
        assertNotNull(log.byId("c_4"), "newest should be retained");

        List<String> recentIds = log.recent(10).stream().map(EventTrace::getId).toList();
        assertEquals(List.of("c_4", "c_3", "c_2"), recentIds, "recent() is newest-first");
    }

    @Test
    void recentAttributionsFiltersToAttributedTraces() {
        RecentEventLog log = log(10);
        log.recordConsumed(EventType.PAGE_VIEW, "pv_attr", "u", T, 0, 0);
        log.recordAttribution(EventType.PAGE_VIEW, "pv_attr", "u", T, 0, 0,
                "attributed → campaign_A (click c1)", "emitted");
        log.recordConsumed(EventType.PAGE_VIEW, "pv_null", "u", T, 0, 1);
        log.recordAttribution(EventType.PAGE_VIEW, "pv_null", "u", T, 0, 1,
                "no attribution (null)", "emitted");

        List<String> ids = log.recentAttributions(10).stream().map(EventTrace::getId).toList();
        assertEquals(List.of("pv_attr"), ids);
    }

    @Test
    void attributionsSurviveEvenWhenLiveTraceEvictedBeforeAttribution() {
        // Reproduces the empty-timeline bug: a page view is attributed only once the
        // watermark passes its event-time + lateness — long after consumption. Under
        // load the capacity-bounded live ring has already evicted the trace by then,
        // so the dedicated attribution index must still surface (and keep inspectable)
        // the attributed event.
        RecentEventLog log = log(3);
        log.recordConsumed(EventType.PAGE_VIEW, "pv_1", "u", T, 0, 7);
        for (int i = 0; i < 10; i++) {
            log.recordConsumed(EventType.PAGE_VIEW, "x_" + i, "u", T, 0, i);
        }
        assertNull(log.byId("pv_1"), "live trace already evicted by the time attribution lands");

        log.recordAttribution(EventType.PAGE_VIEW, "pv_1", "u", T, 0, 7,
                "attributed → campaign_A (click c1)", "emitted");

        assertEquals(List.of("pv_1"),
                log.recentAttributions(50).stream().map(EventTrace::getId).toList(),
                "attribution timeline retains it despite live-ring eviction");
        EventTrace t = log.byId("pv_1");
        assertNotNull(t, "re-inspectable via the attribution index");
        assertEquals(7, t.getOffset());
        assertEquals(List.of("attributed → campaign_A (click c1)", "emitted"),
                t.getSteps().stream().map(RecentEventLog.Step::label).toList());
    }

    @Test
    void disabledLogIsAlwaysEmpty() {
        RecentEventLog log = new RecentEventLog(false, 100);
        log.recordConsumed(EventType.PAGE_VIEW, "pv_1", "u", T, 0, 0);
        log.addStep("pv_1", "emitted");
        assertEquals(0, log.size());
        assertNull(log.byId("pv_1"));
    }

    @Test
    void duplicateIdKeepsOriginalTrace() {
        RecentEventLog log = log(10);
        log.recordConsumed(EventType.AD_CLICK, "c1", "u", T, 0, 0);
        log.recordConsumed(EventType.AD_CLICK, "c1", "u", T, 0, 1); // duplicate id, different offset
        assertEquals(1, log.size());
        EventTrace trace = log.byId("c1");
        assertEquals(0, trace.getOffset(), "original trace retained");
        assertTrue(trace.getSteps().stream().anyMatch(s -> s.label().contains("duplicate")));
    }

    @Test
    void concurrentInsertsStayWithinCapacity() throws InterruptedException {
        RecentEventLog log = log(100);
        int threads = 8;
        int perThread = 500;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            final int base = t * perThread;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < perThread; i++) {
                    log.recordConsumed(EventType.PAGE_VIEW, "pv_" + (base + i), "u", T, 0, base + i);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertTrue(log.size() <= 100, "size must never exceed capacity, was " + log.size());
        assertFalse(log.recent(1000).isEmpty());
    }
}
