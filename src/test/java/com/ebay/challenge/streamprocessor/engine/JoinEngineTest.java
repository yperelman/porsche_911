package com.ebay.challenge.streamprocessor.engine;

import com.ebay.challenge.streamprocessor.TestEvents;
import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.ebay.challenge.streamprocessor.observability.MetricsCounters;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static com.ebay.challenge.streamprocessor.TestEvents.BASE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinEngineTest {

    private static final int P = JoinEngineHarness.PARTITION;
    private static final Instant T = BASE;
    private static final TopicPartition PV_P0 = JoinEngineHarness.PAGE_VIEWS;
    private static final TopicPartition CLICKS_P0 = JoinEngineHarness.CLICKS;

    private static AdClickEvent click(String userId, Instant et, String clickId, String campaignId, long offset) {
        return TestEvents.click(userId, et, clickId, campaignId, P, offset);
    }

    private static PageViewEvent pv(String userId, Instant et, String eventId, long offset) {
        return TestEvents.pv(userId, et, eventId, P, offset);
    }

    @Test
    void afterClickEviction_clickOffsetIsMarkedDoneInTracker() {
        JoinEngineHarness h = JoinEngineHarness.create();
        h.offsets.register(CLICKS_P0, 0L);
        h.offsets.register(CLICKS_P0, 1L);
        h.offsets.register(PV_P0, 0L);

        h.process(click("user_1", T, "click_1", "campaign_A", 0));
        h.advanceBothTopicsTo(T.plus(Duration.ofMinutes(60)));

        Map<TopicPartition, OffsetAndMetadata> committable = h.drainCommittableOffsets();
        assertEquals(1L, committable.get(CLICKS_P0).offset(),
                "click_1's offset 0 should be done after eviction");
    }

    @Test
    void afterEmissionAndFlush_pageViewOffsetIsMarkedDoneInTracker() {
        JoinEngineHarness h = JoinEngineHarness.create();
        h.offsets.register(CLICKS_P0, 0L);
        h.offsets.register(PV_P0, 0L);
        h.offsets.register(CLICKS_P0, 1L);
        h.offsets.register(PV_P0, 1L);

        h.process(click("user_1", T, "click_1", "campaign_A", 0));
        h.process(pv("user_1", T.plus(Duration.ofMinutes(5)), "pv_1", 0));
        h.advanceBothTopicsTo(T.plus(Duration.ofMinutes(40)));

        Map<TopicPartition, OffsetAndMetadata> committable = h.drainCommittableOffsets();
        assertEquals(1L, committable.get(PV_P0).offset(),
                "PV offset 0 should be done after emission + flush");
    }

    @Test
    void initialPageViewUpsert_commitsOffsetOnWrite() {
        JoinEngineHarness h = JoinEngineHarness.create();
        h.offsets.register(PV_P0, 0L);

        h.process(pv("user_1", T.plus(Duration.ofMinutes(5)), "pv_1", 0));

        assertTrue(h.hasWritten("pv_1"), "initial UPSERT should be visible immediately");
        assertEquals(1L, h.drainCommittableOffsets().get(PV_P0).offset(),
                "PV offset is committed on write — correction state is durable in Postgres");
    }

    @Test
    void clickIsRetainedUntilPageViewWindowAndLatenessFenceHavePassed() {
        JoinEngineHarness h = JoinEngineHarness.create();

        h.process(click("user_1", T, "click_1", "campaign_A", 0));
        h.process(pv("user_1", T.plus(Duration.ofMinutes(30)), "pv_1", 0));

        h.advanceBothTopicsTo(T.plus(Duration.ofMinutes(40)));
        assertTrue(h.hasWritten("pv_1"));

        h.advanceBothTopicsTo(T.plus(Duration.ofMinutes(50)));
        AttributedPageView emitted = h.written("pv_1");
        assertEquals("campaign_A", emitted.getAttributedCampaignId());
        assertEquals("click_1", emitted.getAttributedClickId());
    }

    @Test
    void duplicateClickOffsetIsMarkedDoneWithoutBlockingCommitPrefix() {
        JoinEngineHarness h = JoinEngineHarness.create();
        h.offsets.register(CLICKS_P0, 0L);
        h.offsets.register(CLICKS_P0, 1L);
        h.offsets.register(CLICKS_P0, 2L);
        h.offsets.register(PV_P0, 0L);

        h.process(click("user_1", T, "click_1", "campaign_A", 0));
        h.process(click("user_1", T, "click_1", "campaign_A", 1));

        Instant advance = T.plus(Duration.ofMinutes(60));
        h.process(click("user_other", advance, "click_far", "campaign_X", 2));
        h.process(pv("user_other", advance, "pv_far", 0));

        Map<TopicPartition, OffsetAndMetadata> committable = h.drainCommittableOffsets();
        assertEquals(2L, committable.get(CLICKS_P0).offset(),
                "duplicate click offset 1 should not pin commit behind offset 2");
    }

    @Test
    void duplicatePageView_doesNotDoubleCountMetrics() {
        JoinEngineHarness h = JoinEngineHarness.create();
        h.process(click("user_1", T, "click_1", "campaign_A", 0));
        h.process(pv("user_1", T.plus(Duration.ofMinutes(5)), "pv_1", 0));
        h.process(pv("user_1", T.plus(Duration.ofMinutes(5)), "pv_1", 1)); // duplicate redelivery

        assertEquals(1, h.metrics.getPageViewsAttributed(), "distinct page view counted once");
        assertEquals(1, creditFor(h, "campaign_A"), "campaign credited once, not per delivery");
    }

    @Test
    void samePageViewIdDifferentUser_keptAsSeparateRows() {
        // page_view_id is not globally unique; identity is (page_view_id, user_id),
        // so two users sharing an id must not collide into one row.
        JoinEngineHarness h = JoinEngineHarness.create();
        h.process(pv("user_a", T.plus(Duration.ofMinutes(5)), "pv_shared", 0));
        h.process(pv("user_b", T.plus(Duration.ofMinutes(5)), "pv_shared", 1));

        assertEquals(2, h.sink.table.size(), "same page_view_id for different users → two rows");
    }

    @Test
    void retriedClickAfterFlushFailure_reexecutesCorrectionBeforeItCanSucceed() {
        JoinEngineHarness h = JoinEngineHarness.create();
        h.offsets.register(CLICKS_P0, 0L);
        h.process(pv("user_1", T.plus(Duration.ofMinutes(5)), "pv_1", 0));

        h.sink.flushException = new RuntimeException("injected flush failure");
        AdClickEvent click = click("user_1", T, "click_1", "campaign_A", 0);
        assertThrows(RuntimeException.class, () -> h.process(click));

        h.sink.flushException = null;
        h.process(click);

        assertEquals(2, h.sink.correctionCalls,
                "a retry must rerun the idempotent database correction");
        assertEquals(3, h.sink.flushCount,
                "page-view flush plus both click correction flush attempts");
        assertEquals("campaign_A", h.written("pv_1").getAttributedCampaignId());
        assertTrue(h.drainCommittableOffsets().isEmpty(),
                "the retained click source offset must remain replayable until watermark eviction");
    }

    @Test
    void duplicatePageViewOffsetIsMarkedDoneWithoutOverwritingOriginal() {
        JoinEngineHarness h = JoinEngineHarness.create();
        h.offsets.register(CLICKS_P0, 0L);
        h.offsets.register(CLICKS_P0, 1L);
        h.offsets.register(PV_P0, 0L);
        h.offsets.register(PV_P0, 1L);
        h.offsets.register(PV_P0, 2L);

        h.process(click("user_1", T, "click_1", "campaign_A", 0));
        h.process(pv("user_1", T.plus(Duration.ofMinutes(5)), "pv_1", 0));
        h.process(pv("user_1", T.plus(Duration.ofMinutes(5)), "pv_1", 1));

        Instant advance = T.plus(Duration.ofMinutes(40));
        h.process(click("user_other", advance, "click_far", "campaign_X", 1));
        h.process(pv("user_other", advance, "pv_far", 2));

        Map<TopicPartition, OffsetAndMetadata> committable = h.drainCommittableOffsets();
        assertEquals(3L, committable.get(PV_P0).offset(),
                "offsets 0, 1 (duplicate) and 2 are all committed on write");
        assertTrue(h.hasWritten("pv_1"));
        assertEquals("campaign_A", h.written("pv_1").getAttributedCampaignId(),
                "duplicate write must not regress the original attribution");
    }

    @Test
    void clickBeforePageView_attributesToTheClick_immediately() {
        JoinEngineHarness h = JoinEngineHarness.create();

        h.process(click("user_1", T, "click_1", "campaign_A", 0));
        h.process(pv("user_1", T.plus(Duration.ofMinutes(5)), "pv_1", 0));

        AttributedPageView emitted = h.written("pv_1");
        assertEquals("campaign_A", emitted.getAttributedCampaignId());
        assertEquals("click_1", emitted.getAttributedClickId());
    }

    @Test
    void multipleClicksInWindow_picksLatestClick() {
        JoinEngineHarness h = JoinEngineHarness.create();

        h.process(click("user_1", T, "click_1a", "campaign_A", 0));
        h.process(click("user_1", T.plus(Duration.ofMinutes(10)), "click_1b", "campaign_B", 1));
        h.process(pv("user_1", T.plus(Duration.ofMinutes(15)), "pv_1", 0));

        h.advanceBothTopicsTo(T.plus(Duration.ofMinutes(40)));

        AttributedPageView emitted = h.written("pv_1");
        assertEquals("campaign_B", emitted.getAttributedCampaignId());
        assertEquals("click_1b", emitted.getAttributedClickId());
    }

    @Test
    void clickOutsideWindow_isNotAttributed() {
        JoinEngineHarness h = JoinEngineHarness.create();

        h.process(click("user_1", T, "click_1", "campaign_A", 0));
        h.process(pv("user_1", T.plus(Duration.ofMinutes(35)), "pv_1", 0));

        h.advanceBothTopicsTo(T.plus(Duration.ofMinutes(60)));

        AttributedPageView emitted = h.written("pv_1");
        assertNull(emitted.getAttributedCampaignId());
        assertNull(emitted.getAttributedClickId());
    }

    @Test
    void outOfOrderClickArrival_withinLateness_isStillAttributed() {
        JoinEngineHarness h = JoinEngineHarness.create();

        h.process(pv("user_1", T.plus(Duration.ofMinutes(15)), "pv_1", 0));
        AttributedPageView first = h.written("pv_1");
        assertNull(first.getAttributedCampaignId());
        assertNull(first.getAttributedClickId());

        h.process(click("user_1", T.plus(Duration.ofMinutes(10)), "click_1", "campaign_A", 0));

        AttributedPageView emitted = h.written("pv_1");
        assertEquals("campaign_A", emitted.getAttributedCampaignId());
        assertEquals("click_1", emitted.getAttributedClickId());
        assertEquals(1, h.writesFor("pv_1").size(), "page view is written once");
        assertEquals(1, h.sink.correctionCount, "late click should produce a correction UPDATE");
    }

    @Test
    void clickAtFinalizationCutoff_isDeadLettered_notAcceptedAfterPageViewFinalized() {
        JoinEngineHarness h = JoinEngineHarness.create();

        h.process(pv("user_1", T, "pv_1", 0));
        h.advanceBothTopicsTo(T.plus(Duration.ofMinutes(15)));
        h.process(click("user_1", T, "click_boundary", "campaign_A", 0));

        AttributedPageView emitted = h.written("pv_1");
        assertNull(emitted.getAttributedCampaignId());
        assertNull(emitted.getAttributedClickId());
        assertEquals(1, h.writesFor("pv_1").size(),
                "boundary click must not correct a page view already finalized at the same cutoff");
        assertEquals(1, h.dlq.deadLettered.size());
        assertEquals("ad_clicks", h.dlq.deadLettered.get(0).sourceTopic());
    }

    @Test
    void olderLateClick_doesNotOverwriteNewerAttribution() {
        JoinEngineHarness h = JoinEngineHarness.create();

        h.process(pv("user_1", T.plus(Duration.ofMinutes(20)), "pv_1", 0));
        h.process(click("user_1", T.plus(Duration.ofMinutes(10)), "click_new", "campaign_NEW", 0));
        h.process(click("user_1", T.plus(Duration.ofMinutes(5)), "click_old", "campaign_OLD", 1));

        AttributedPageView emitted = h.written("pv_1");
        assertEquals("campaign_NEW", emitted.getAttributedCampaignId());
        assertEquals("click_new", emitted.getAttributedClickId());
        assertEquals(1, h.writesFor("pv_1").size(), "page view is written once");
        assertEquals(1, h.sink.correctionCount,
                "only the newer click corrects; the older click is a no-op UPDATE");
    }

    @Test
    void campaignCreditReflectsInitialAttribution_notCorrections() {
        JoinEngineHarness h = JoinEngineHarness.create();

        // PV arrives after an eligible click → initial attribution credits campaign_OLD.
        h.process(click("user_1", T.plus(Duration.ofMinutes(5)), "click_old", "campaign_OLD", 0));
        h.process(pv("user_1", T.plus(Duration.ofMinutes(20)), "pv_1", 0));
        // A newer eligible click corrects the row to campaign_NEW, but credit does not move
        // (correction is a set-based UPDATE with no per-row result).
        h.process(click("user_1", T.plus(Duration.ofMinutes(10)), "click_new", "campaign_NEW", 1));

        assertEquals("campaign_NEW", h.written("pv_1").getAttributedCampaignId(),
                "row attribution is corrected to the newer click");
        assertEquals(1, creditFor(h, "campaign_OLD"), "credit stays with the initial attribution");
        assertEquals(0, creditFor(h, "campaign_NEW"), "corrections do not move campaign credit");
    }

    private static long creditFor(JoinEngineHarness h, String campaignId) {
        return h.metrics.topCampaigns(100).stream()
                .filter(c -> c.campaignId().equals(campaignId))
                .mapToLong(MetricsCounters.CampaignCredit::count)
                .findFirst().orElse(0L);
    }

    @Test
    void veryLatePageView_isPublishedToDeadLetter_notEmitted() {
        JoinEngineHarness h = JoinEngineHarness.create();
        h.advanceBothTopicsTo(T.plus(Duration.ofMinutes(60)));
        int writtenBefore = h.sink.written.size();

        h.process(pv("user_1", T.plus(Duration.ofMinutes(10)), "pv_late", 1));

        assertTrue(!h.hasWritten("pv_late"));
        assertEquals(writtenBefore, h.sink.written.size());
        assertEquals(1, h.dlq.deadLettered.size());
        assertEquals("page_views", h.dlq.deadLettered.get(0).sourceTopic());
        assertTrue(h.dlq.deadLettered.get(0).payload().contains("pv_late"),
                "payload should embed the event id");
    }

    @Test
    void pageViewWithNoClick_emitsWithNullAttribution_afterWatermarkAdvance() {
        JoinEngineHarness h = JoinEngineHarness.create();

        h.process(pv("user_lonely", T.plus(Duration.ofMinutes(5)), "pv_lonely", 0));
        h.advanceBothTopicsTo(T.plus(Duration.ofMinutes(30)));

        AttributedPageView emitted = h.written("pv_lonely");
        assertNull(emitted.getAttributedCampaignId());
        assertNull(emitted.getAttributedClickId());
    }

    @Test
    void sinkFlushFailure_propagatesOutOfProcessCall() {
        JoinEngineHarness h = JoinEngineHarness.create();
        h.process(click("u", T, "c", "X", 0));
        h.sink.flushException = new RuntimeException("flush failed after retries");

        RuntimeException thrown = assertThrows(RuntimeException.class, () ->
                h.process(pv("u", T.plus(Duration.ofMinutes(5)), "pv", 0)));
        assertTrue(thrown.getMessage().contains("flush failed"),
                "sink flush exception must propagate without being swallowed");
    }
}
