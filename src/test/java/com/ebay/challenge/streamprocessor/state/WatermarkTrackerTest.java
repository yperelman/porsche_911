package com.ebay.challenge.streamprocessor.state;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WatermarkTrackerTest {

    private static final String CLICKS = "ad_clicks";
    private static final String PAGE_VIEWS = "page_views";

    @Test
    void constructor_enforcesReadmeLatenessBounds() {
        // README: "Configurable allowed lateness (max 15 minutes)".
        for (int valid : List.of(0, 1, 15)) {
            new WatermarkTracker(valid, "ad_clicks", "page_views");
        }
        assertThrows(IllegalArgumentException.class, () -> new WatermarkTracker(-1, "ad_clicks", "page_views"));
        IllegalArgumentException above = assertThrows(IllegalArgumentException.class,
                () -> new WatermarkTracker(16, "ad_clicks", "page_views"));
        assertTrue(above.getMessage().contains("15"),
                "exception message should reference the cap");
    }

    @Test
    void updateWatermark_advancesMonotonically() {
        WatermarkTracker tracker = new WatermarkTracker(15, "ad_clicks", "page_views");
        Instant t1 = Instant.parse("2024-01-01T12:00:00Z");
        Instant t0 = Instant.parse("2024-01-01T11:00:00Z");
        Instant t2 = Instant.parse("2024-01-01T12:30:00Z");

        tracker.updateWatermark(0, CLICKS, t1);
        assertEquals(t1, tracker.getWatermark(0, CLICKS));

        tracker.updateWatermark(0, CLICKS, t0); // earlier — must not go backward
        assertEquals(t1, tracker.getWatermark(0, CLICKS));

        tracker.updateWatermark(0, CLICKS, t2); // later — advances
        assertEquals(t2, tracker.getWatermark(0, CLICKS));
    }

    @Test
    void joinedWatermark_isMinOfTopicWatermarksForThePartition() {
        WatermarkTracker tracker = new WatermarkTracker(15, "ad_clicks", "page_views");
        Instant clickWm = Instant.parse("2024-01-01T12:30:00Z");
        Instant pvWm = Instant.parse("2024-01-01T12:00:00Z");

        tracker.updateWatermark(0, CLICKS, clickWm);
        tracker.updateWatermark(0, PAGE_VIEWS, pvWm);

        assertEquals(pvWm, tracker.getJoinedWatermark(0).orElseThrow());
    }

    @Test
    void joinedWatermark_isEmpty_whenAnyTopicMissing() {
        WatermarkTracker tracker = new WatermarkTracker(15, "ad_clicks", "page_views");
        // Only the clicks topic has seen events; PV side is silent.
        tracker.updateWatermark(0, CLICKS, Instant.parse("2024-01-01T12:30:00Z"));

        // Joined watermark blocks until both sides have produced events
        // (idle-source detection handles the legitimate-silence case separately).
        assertTrue(tracker.getJoinedWatermark(0).isEmpty());
    }

    @Test
    void isTooLate_returnsTrue_belowJoinedWatermarkMinusLateness() {
        WatermarkTracker tracker = new WatermarkTracker(15, "ad_clicks", "page_views");
        Instant wm = Instant.parse("2024-01-01T13:00:00Z");
        tracker.updateWatermark(0, CLICKS, wm);
        tracker.updateWatermark(0, PAGE_VIEWS, wm);

        // Cutoff = wm - 15 min = 12:45. Boundary is inclusive because a PV at
        // 12:45 is finalized when watermark reaches 13:00, so a 12:45 event can
        // no longer safely correct it.
        assertTrue(tracker.isTooLate(0, Instant.parse("2024-01-01T12:30:00Z")));
        assertTrue(tracker.isTooLate(0, Instant.parse("2024-01-01T12:45:00Z")));
        assertFalse(tracker.isTooLate(0, Instant.parse("2024-01-01T12:50:00Z")));
        assertFalse(tracker.isTooLate(0, Instant.parse("2024-01-01T13:30:00Z"))); // future, not late
    }

    @Test
    void isTooLate_returnsFalse_whenJoinedWatermarkUninitialized() {
        WatermarkTracker tracker = new WatermarkTracker(15, "ad_clicks", "page_views");
        // joined watermark is empty → nothing can be too late yet
        assertFalse(tracker.isTooLate(0, Instant.parse("2024-01-01T12:00:00Z")));
    }

    @Test
    void idleSource_isExcludedFromJoinedWatermarkMin() {
        WatermarkTracker tracker = new WatermarkTracker(15, "ad_clicks", "page_views");

        Instant clickWm = Instant.parse("2024-01-01T13:00:00Z");
        Instant pvWm = Instant.parse("2024-01-01T12:30:00Z");
        tracker.updateWatermark(0, CLICKS, clickWm);
        tracker.updateWatermark(0, PAGE_VIEWS, pvWm);

        // Both seen recently → joined = min = pv
        assertEquals(pvWm, tracker.getJoinedWatermark(0).orElseThrow());

        tracker.markIdleIfCaughtUp(0, PAGE_VIEWS, true, false);
        tracker.updateWatermark(0, CLICKS, clickWm.plus(Duration.ofMinutes(1)));  // keep clicks fresh

        // A caught-up, unpaused PAGE_VIEWS source is idle; clicks carry the frontier.
        assertEquals(clickWm.plus(Duration.ofMinutes(1)), tracker.getJoinedWatermark(0).orElseThrow());
    }

    @Test
    void backloggedOrPausedSource_isNeverMarkedIdle() {
        WatermarkTracker tracker = new WatermarkTracker(15, "ad_clicks", "page_views");
        Instant initial = Instant.parse("2024-01-01T12:00:00Z");
        Instant later = Instant.parse("2024-01-01T13:00:00Z");

        tracker.updateWatermark(0, CLICKS, later);
        tracker.updateWatermark(0, PAGE_VIEWS, initial);

        tracker.markIdleIfCaughtUp(0, PAGE_VIEWS, false, false);
        assertEquals(initial, tracker.getJoinedWatermark(0).orElseThrow(), "backlog must keep constraining the join");

        tracker.markIdleIfCaughtUp(0, PAGE_VIEWS, true, true);
        assertEquals(initial, tracker.getJoinedWatermark(0).orElseThrow(), "paused source must not be treated as idle");
    }

    @Test
    void joinedWatermark_doesNotRegressWhenIdleSourceResumes() {
        WatermarkTracker tracker = new WatermarkTracker(15, "ad_clicks", "page_views");
        Instant initial = Instant.parse("2024-01-01T12:00:00Z");
        Instant advanced = Instant.parse("2024-01-01T13:00:00Z");

        tracker.updateWatermark(0, CLICKS, initial);
        tracker.updateWatermark(0, PAGE_VIEWS, initial);
        tracker.markIdleIfCaughtUp(0, PAGE_VIEWS, true, false);
        tracker.updateWatermark(0, CLICKS, advanced);
        assertEquals(advanced, tracker.getJoinedWatermark(0).orElseThrow());

        tracker.updateWatermark(0, PAGE_VIEWS, initial.plus(Duration.ofMinutes(10)));
        assertEquals(advanced, tracker.getJoinedWatermark(0).orElseThrow(),
                "resuming an idle source must not move the established frontier backward");
    }

    @Test
    void joinedWatermark_isPerPartition() {
        WatermarkTracker tracker = new WatermarkTracker(15, "ad_clicks", "page_views");
        Instant earlier = Instant.parse("2024-01-01T12:00:00Z");
        Instant later = Instant.parse("2024-01-01T12:30:00Z");

        tracker.updateWatermark(0, CLICKS, earlier);
        tracker.updateWatermark(0, PAGE_VIEWS, earlier);
        tracker.updateWatermark(1, CLICKS, later);
        tracker.updateWatermark(1, PAGE_VIEWS, later);

        assertEquals(earlier, tracker.getJoinedWatermark(0).orElseThrow());
        assertEquals(later, tracker.getJoinedWatermark(1).orElseThrow());
    }
}
