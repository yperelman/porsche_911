package com.ebay.challenge.streamprocessor.engine;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static com.ebay.challenge.streamprocessor.TestEvents.BASE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Invalid input must be quarantined to the dead-letter topic, not crash the
 * processing thread (which today becomes a poison pill: the record is retried
 * forever and its offset never commits).
 *
 * <p>Bug 4: a missing {@code event_time} NPEs in the watermark path.
 * Bug 5: a null {@code user_id} NPEs in the click store / fails the sink write.
 */
class JoinEngineValidationTest {

    private static final int P = JoinEngineHarness.PARTITION;
    private static final Instant T = BASE;

    @Test
    void clickWithNullEventTime_isDeadLettered_notCrash() {
        JoinEngineHarness h = JoinEngineHarness.create();
        AdClickEvent click = AdClickEvent.builder()
                .userId("u").eventTime(null).campaignId("camp").clickId("k")
                .partition(P).offset(0).build();

        assertDoesNotThrow(() -> h.process(click));
        assertEquals(1, h.dlq.deadLettered.size(), "missing event_time must be dead-lettered");
    }

    @Test
    void clickWithNullUserId_isDeadLettered_notCrash() {
        JoinEngineHarness h = JoinEngineHarness.create();
        AdClickEvent click = AdClickEvent.builder()
                .userId(null).eventTime(T).campaignId("camp").clickId("k")
                .partition(P).offset(0).build();

        assertDoesNotThrow(() -> h.process(click));
        assertEquals(1, h.dlq.deadLettered.size(), "null user_id must be dead-lettered");
    }

    @Test
    void pageViewWithNullUserId_isDeadLettered_notWritten() {
        JoinEngineHarness h = JoinEngineHarness.create();
        PageViewEvent pv = PageViewEvent.builder()
                .userId(null).eventTime(T).url("http://x").eventId("pv_nu")
                .partition(P).offset(0).build();

        assertDoesNotThrow(() -> h.process(pv));
        assertFalse(h.hasWritten("pv_nu"), "null user_id must not be written");
        assertEquals(1, h.dlq.deadLettered.size(), "null user_id must be dead-lettered");
    }

    /**
     * Bug 6: an event whose {@code event_time} is far ahead of wall-clock (producer
     * clock skew) currently advances the monotonic joined watermark permanently,
     * stranding the partition so every later real-time event is dropped as "too late".
     * Far-future events must be rejected (dead-lettered) without touching the watermark.
     */
    @Test
    void clickFarInFuture_isRejected_doesNotPoisonWatermark() {
        Instant now = Instant.parse("2024-01-01T13:00:00Z");
        JoinEngineHarness h = JoinEngineHarness.createAt(now);

        AdClickEvent future = AdClickEvent.builder()
                .userId("uF").eventTime(now.plus(Duration.ofDays(1)))
                .campaignId("campFUT").clickId("kF").partition(P).offset(0).build();
        assertDoesNotThrow(() -> h.process(future));
        assertEquals(1, h.dlq.deadLettered.size(), "far-future event must be dead-lettered");

        // Watermark not poisoned: a normal page view at 'now' still gets written.
        PageViewEvent pv = PageViewEvent.builder()
                .userId("uF").eventTime(now).url("http://x").eventId("pv_f")
                .partition(P).offset(1).build();
        h.process(pv);
        assertTrue(h.hasWritten("pv_f"), "normal event must not be dropped by a stranded watermark");
        assertNull(h.written("pv_f").getAttributedCampaignId(), "rejected future click must not attribute");
    }
}
