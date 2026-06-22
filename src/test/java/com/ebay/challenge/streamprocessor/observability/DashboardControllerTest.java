package com.ebay.challenge.streamprocessor.observability;

import com.ebay.challenge.streamprocessor.observability.DashboardController.DashboardSnapshot;
import com.ebay.challenge.streamprocessor.observability.DashboardController.EventSummary;
import com.ebay.challenge.streamprocessor.observability.RecentEventLog.EventTrace;
import com.ebay.challenge.streamprocessor.observability.RecentEventLog.EventType;
import com.ebay.challenge.streamprocessor.output.BackpressureCoordinator;
import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DashboardControllerTest {

    private static final Instant NOW = Instant.parse("2024-01-01T12:30:00Z");

    private DashboardController controller(WatermarkTracker watermarks, RecentEventLog eventLog, int partitions) {
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        when(registry.getAllListenerContainers()).thenReturn(List.of());
        BackpressureCoordinator backpressure = new BackpressureCoordinator(registry, 5);
        return new DashboardController(
                new ClickStateStore(30),
                watermarks,
                backpressure,
                new MetricsCounters(),
                eventLog,
                Clock.fixed(NOW, ZoneOffset.UTC),
                partitions);
    }

    @Test
    void lagIsNullUntilAdvanced_thenProcessingStalenessSinceLastAdvance() {
        // WatermarkTracker advances on a clock 2 min behind the controller's NOW, so the
        // partition's last-advance is NOW-2min → staleness = 120s. Partition 1 never advances.
        Clock wmClock = Clock.fixed(NOW.minus(Duration.ofMinutes(2)), ZoneOffset.UTC);
        WatermarkTracker watermarks = new WatermarkTracker(15, "ad_clicks", "page_views", wmClock);
        Instant wm = NOW.minus(Duration.ofMinutes(2));
        watermarks.updateWatermark(0, "ad_clicks", wm);
        watermarks.updateWatermark(0, "page_views", wm);

        DashboardSnapshot snap = controller(watermarks, new RecentEventLog(true, 100), 2).snapshot();

        assertEquals(wm, snap.joinedWatermarkByPartition().get(0));
        assertNull(snap.joinedWatermarkByPartition().get(1), "uninitialized watermark reports null timestamp");
        assertEquals(120L, snap.lagSecondsByPartition().get(0), "staleness = wall-clock since last advance");
        assertNull(snap.lagSecondsByPartition().get(1), "partition that never advanced reports null lag");
    }

    @Test
    void lagIsProcessingStaleness_notEventTimeSkew() {
        // Even a far-future-dated watermark reports small positive staleness (time since
        // advance), never a huge/negative event-time skew. Advance recorded 30s before NOW.
        Clock wmClock = Clock.fixed(NOW.minus(Duration.ofSeconds(30)), ZoneOffset.UTC);
        WatermarkTracker watermarks = new WatermarkTracker(15, "ad_clicks", "page_views", wmClock);
        Instant future = NOW.plus(Duration.ofDays(10));
        watermarks.updateWatermark(0, "ad_clicks", future);
        watermarks.updateWatermark(0, "page_views", future);

        DashboardSnapshot snap = controller(watermarks, new RecentEventLog(true, 100), 1).snapshot();

        assertEquals(30L, snap.lagSecondsByPartition().get(0),
                "lag is time since last advance, independent of the watermark's event-time epoch");
    }

    @Test
    void recentEventsReturnsNewestFirstSummaries() {
        RecentEventLog log = new RecentEventLog(true, 100);
        log.recordConsumed(EventType.PAGE_VIEW, "pv_1", "user_1", NOW, 0, 0);
        log.addStep("pv_1", "buffered (awaiting watermark)");
        log.recordConsumed(EventType.AD_CLICK, "c_1", "user_2", NOW, 1, 0);
        log.addStep("c_1", "stored in click state");

        List<EventSummary> recent = controller(new WatermarkTracker(15, "ad_clicks", "page_views"), log, 3).recentEvents(10);

        assertEquals(2, recent.size());
        assertEquals("c_1", recent.get(0).id(), "newest first");
        assertEquals("stored in click state", recent.get(0).latestStep());
        assertEquals("pv_1", recent.get(1).id());
        assertEquals("buffered (awaiting watermark)", recent.get(1).latestStep());
    }

    @Test
    void eventByIdReturnsJourneyOr404() {
        RecentEventLog log = new RecentEventLog(true, 100);
        log.recordConsumed(EventType.PAGE_VIEW, "pv_1", "user_1", NOW, 0, 0);
        log.addStep("pv_1", "emitted");
        DashboardController controller = controller(new WatermarkTracker(15, "ad_clicks", "page_views"), log, 3);

        ResponseEntity<EventTrace> found = controller.eventById("pv_1");
        assertEquals(HttpStatus.OK, found.getStatusCode());
        assertEquals("pv_1", found.getBody().getId());
        assertEquals(List.of("consumed", "emitted"),
                found.getBody().getSteps().stream().map(RecentEventLog.Step::label).toList());

        ResponseEntity<EventTrace> missing = controller.eventById("nope");
        assertEquals(HttpStatus.NOT_FOUND, missing.getStatusCode());
    }
}
