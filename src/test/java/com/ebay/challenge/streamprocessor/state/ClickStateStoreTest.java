package com.ebay.challenge.streamprocessor.state;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickStateStoreTest {

    private static AdClickEvent click(String userId, Instant eventTime, String clickId, String campaignId) {
        return AdClickEvent.builder()
                .userId(userId)
                .eventTime(eventTime)
                .clickId(clickId)
                .campaignId(campaignId)
                .build();
    }

    @Test
    void addAndFindWithinWindow_returnsTheClick() {
        ClickStateStore store = new ClickStateStore(30);
        Instant clickTime = Instant.parse("2024-01-01T12:00:00Z");
        AdClickEvent c = click("user_1", clickTime, "click_1", "campaign_A");

        store.addClick(c);

        Instant pageViewTime = clickTime.plus(Duration.ofMinutes(5));
        AdClickEvent found = store.findAttributableClick("user_1", pageViewTime);

        assertEquals(c, found);
    }

    @Test
    void multipleClicksInWindow_picksLatest() {
        ClickStateStore store = new ClickStateStore(30);
        ClickStateStore reverseInsertionStore = new ClickStateStore(30);
        Instant base = Instant.parse("2024-01-01T12:00:00Z");
        AdClickEvent older = click("user_1", base, "click_1", "campaign_A");
        AdClickEvent newer = click("user_1", base.plus(Duration.ofMinutes(10)), "click_2", "campaign_B");

        store.addClick(older);
        store.addClick(newer);
        reverseInsertionStore.addClick(newer);
        reverseInsertionStore.addClick(older);

        Instant pageViewTime = base.plus(Duration.ofMinutes(15));
        assertEquals(newer, store.findAttributableClick("user_1", pageViewTime));
        assertEquals(newer, reverseInsertionStore.findAttributableClick("user_1", pageViewTime));
    }

    @Test
    void findOutsideWindow_returnsNull() {
        ClickStateStore store = new ClickStateStore(30);
        Instant clickTime = Instant.parse("2024-01-01T12:00:00Z");
        store.addClick(click("user_1", clickTime, "click_1", "campaign_A"));

        Instant pageViewTime = clickTime.plus(Duration.ofMinutes(35));
        AdClickEvent found = store.findAttributableClick("user_1", pageViewTime);

        assertNull(found);
    }

    @Test
    void concurrentAdds_areSafe() throws InterruptedException {
        ClickStateStore store = new ClickStateStore(30);
        Instant base = Instant.parse("2024-01-01T12:00:00Z");
        int threads = 4;
        int clicksPerThread = 100;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            final int threadIdx = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < clicksPerThread; i++) {
                        String user = "user_" + (i % 10);
                        Instant et = base.plus(Duration.ofMillis(threadIdx * 1000L + i));
                        String clickId = "click_" + threadIdx + "_" + i;
                        store.addClick(click(user, et, clickId, "campaign_X"));
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdown();

        assertEquals((long) threads * clicksPerThread, store.getTotalClickCount());
    }

    @Test
    void getTotalClickCount_reflectsAddedAndEvicted() {
        ClickStateStore store = new ClickStateStore(30);
        Instant base = Instant.parse("2024-01-01T12:00:00Z");

        assertEquals(0, store.getTotalClickCount());

        store.addClick(click("user_1", base, "click_1", "campaign_A"));
        store.addClick(click("user_2", base, "click_2", "campaign_B"));
        assertEquals(2, store.getTotalClickCount());

        // Duplicate click_id doesn't change count
        store.addClick(click("user_1", base, "click_1", "campaign_A"));
        assertEquals(2, store.getTotalClickCount());

        var evictedReturn = store.evictOldClicks(0, base.plusSeconds(1));
        assertEquals(2, evictedReturn.size());
        assertEquals(0, store.getTotalClickCount());
    }

    @Test
    void evictForPartition_onlyTouchesClicksOnThatPartition() {
        // A partition advancing its watermark must NOT cause other partitions' clicks to be
        // evicted. Otherwise a fast-flushing partition silently breaks a slow partition's
        // attribution because the slow partition's state is wiped before its PVs drain.
        ClickStateStore store = new ClickStateStore(30);
        Instant t = Instant.parse("2024-01-01T12:00:00Z");
        AdClickEvent onP0 = AdClickEvent.builder()
                .userId("user_p0").eventTime(t).clickId("c_p0").campaignId("A")
                .partition(0).offset(0).build();
        AdClickEvent onP1 = AdClickEvent.builder()
                .userId("user_p1").eventTime(t).clickId("c_p1").campaignId("B")
                .partition(1).offset(0).build();
        store.addClick(onP0);
        store.addClick(onP1);
        assertEquals(1, store.getClickCount(0));
        assertEquals(1, store.getClickCount(1));

        // Partition 1 evicts everything < t+1s. Only user_p1's click should go.
        var evicted = store.evictOldClicks(1, t.plusSeconds(1));

        assertEquals(1, evicted.size(), "eviction on partition 1 must only touch partition 1 clicks");
        assertEquals("c_p1", evicted.get(0).getClickId());
        assertEquals(1, store.getClickCount(0));
        assertEquals(0, store.getClickCount(1));
        // user_p0's click on partition 0 must still be attributable.
        assertNotNull(store.findAttributableClick("user_p0", t.plus(Duration.ofMinutes(5))),
                "partition 0's click must NOT have been evicted by partition 1's flush");
    }

    @Test
    void differentClickIds_atSameEventTime_areBothStored() {
        ClickStateStore store = new ClickStateStore(30);
        Instant t = Instant.parse("2024-01-01T12:00:00Z");
        store.addClick(click("user_1", t, "click_X", "campaign_A"));
        store.addClick(click("user_1", t, "click_Y", "campaign_B"));

        var evicted = store.evictOldClicks(0, t.plusSeconds(1));

        assertEquals(2, evicted.size());
    }

    @Test
    void clickAtExactPageViewTime_isAttributable() {
        // Boundary case: a click and its page view share the exact same event_time.
        // The lookup is inclusive of click.event_time == pageViewTime.
        ClickStateStore store = new ClickStateStore(30);
        Instant t = Instant.parse("2024-01-01T12:00:00Z");
        AdClickEvent c = click("user_1", t, "click_X", "campaign_A");
        store.addClick(c);

        AdClickEvent found = store.findAttributableClick("user_1", t);

        assertEquals(c, found);
    }

    @Test
    void multipleClicksAtSameEventTime_returnsExactlyOneAttribution() {
        // Two distinct clicks at the exact same event_time. The README does not specify
        // a tie-break rule for this case (and the input model has no field linking a
        // click to a page view causally). We guarantee the engine returns exactly one
        // of the candidates — deterministically, since the underlying map is sorted —
        // and never null. Which one is documented as a product-question note in DESIGN.md.
        ClickStateStore store = new ClickStateStore(30);
        Instant t = Instant.parse("2024-01-01T12:00:00Z");
        AdClickEvent c1 = click("user_1", t, "click_A", "campaign_X");
        AdClickEvent c2 = click("user_1", t, "click_B", "campaign_Y");
        store.addClick(c1);
        store.addClick(c2);

        AdClickEvent found = store.findAttributableClick("user_1", t.plus(Duration.ofMinutes(5)));

        assertNotNull(found);
        assertTrue(found.equals(c1) || found.equals(c2),
                "must attribute to one of the same-second candidates");
        // Replay-stability: invoking again returns the same result deterministically.
        assertEquals(found, store.findAttributableClick("user_1", t.plus(Duration.ofMinutes(5))));
    }

    @Test
    void multipleClicksAtSameEventTime_pickedOverOlderClickInWindow() {
        // Same-second clicks should still beat strictly-earlier clicks in the window
        // (because their event_time is greater). Pins that the sentinel-free lookup
        // didn't accidentally regress the "pick latest event_time" rule at boundary.
        ClickStateStore store = new ClickStateStore(30);
        Instant earlier = Instant.parse("2024-01-01T12:00:00Z");
        Instant tie = earlier.plus(Duration.ofMinutes(10));
        store.addClick(click("user_1", earlier, "click_old", "campaign_OLD"));
        store.addClick(click("user_1", tie, "click_A", "campaign_X"));
        store.addClick(click("user_1", tie, "click_B", "campaign_Y"));

        AdClickEvent found = store.findAttributableClick("user_1", tie);

        assertNotNull(found);
        assertEquals(tie, found.getEventTime(),
                "must pick one of the same-second clicks at the latest event_time, not the older click");
        assertTrue(found.getClickId().equals("click_A") || found.getClickId().equals("click_B"));
    }

    @Test
    void duplicateClickId_isDeduped() {
        ClickStateStore store = new ClickStateStore(30);
        Instant t = Instant.parse("2024-01-01T12:00:00Z");
        AdClickEvent c1 = click("user_1", t, "click_X", "campaign_A");
        AdClickEvent c2 = click("user_1", t, "click_X", "campaign_A");

        store.addClick(c1);
        store.addClick(c2);

        var evicted = store.evictOldClicks(0, t.plusSeconds(1));

        assertEquals(1, evicted.size(), "duplicate click_id must not be double-counted");
    }

    @Test
    void evictOldClicks_removesClicksOlderThanCutoff() {
        ClickStateStore store = new ClickStateStore(30);
        Instant base = Instant.parse("2024-01-01T12:00:00Z");
        AdClickEvent old1 = click("user_1", base, "click_1", "campaign_A");
        AdClickEvent old2 = click("user_2", base.plus(Duration.ofMinutes(5)), "click_2", "campaign_B");
        AdClickEvent kept = click("user_3", base.plus(Duration.ofMinutes(20)), "click_3", "campaign_C");

        store.addClick(old1);
        store.addClick(old2);
        store.addClick(kept);

        var evicted = store.evictOldClicks(0, base.plus(Duration.ofMinutes(10)));

        assertEquals(2, evicted.size());
        // Looking up the evicted users should return null
        assertNull(store.findAttributableClick("user_1", base.plus(Duration.ofMinutes(15))));
        assertNull(store.findAttributableClick("user_2", base.plus(Duration.ofMinutes(15))));
        // Kept click is still attributable
        AdClickEvent stillThere = store.findAttributableClick("user_3", base.plus(Duration.ofMinutes(25)));
        assertNotNull(stillThere);
        assertEquals(kept, stillThere);
    }
}
