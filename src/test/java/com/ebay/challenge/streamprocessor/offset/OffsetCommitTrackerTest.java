package com.ebay.challenge.streamprocessor.offset;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OffsetCommitTrackerTest {

    private static final TopicPartition CLICKS_0 = new TopicPartition("ad_clicks", 0);

    private static final TopicPartition CLICKS_1 = new TopicPartition("ad_clicks", 1);
    private static final TopicPartition PV_0 = new TopicPartition("page_views", 0);

    @Test
    void registerThenMarkDone_yieldsCommittableOffsetOnePastLast() {
        OffsetCommitTracker tracker = new OffsetCommitTracker();

        tracker.register(CLICKS_0, 0L);
        tracker.register(CLICKS_0, 1L);
        tracker.register(CLICKS_0, 2L);

        tracker.markDone(CLICKS_0, 0L);
        tracker.markDone(CLICKS_0, 1L);
        tracker.markDone(CLICKS_0, 2L);

        Map<TopicPartition, OffsetAndMetadata> committable = tracker.drainCommittableBatch().offsets();

        assertEquals(1, committable.size());
        assertEquals(3L, committable.get(CLICKS_0).offset());
    }

    @Test
    void gap_blocksCommit_beyondTheGap() {
        OffsetCommitTracker tracker = new OffsetCommitTracker();

        tracker.register(CLICKS_0, 0L);
        tracker.register(CLICKS_0, 1L);
        tracker.register(CLICKS_0, 2L);
        tracker.register(CLICKS_0, 3L);

        tracker.markDone(CLICKS_0, 0L);
        tracker.markDone(CLICKS_0, 1L);
        // 2 left un-done
        tracker.markDone(CLICKS_0, 3L);

        Map<TopicPartition, OffsetAndMetadata> committable = tracker.drainCommittableBatch().offsets();
        // Only the prefix [0,1] is committable → commit next offset 2.
        assertEquals(2L, committable.get(CLICKS_0).offset());
    }

    @Test
    void drainAfterGap_advancesOnceGapIsClosed() {
        OffsetCommitTracker tracker = new OffsetCommitTracker();
        for (long o = 0; o < 4; o++) {
            tracker.register(CLICKS_0, o);
        }
        tracker.markDone(CLICKS_0, 0L);
        tracker.markDone(CLICKS_0, 1L);
        tracker.markDone(CLICKS_0, 3L);
        Map<TopicPartition, OffsetAndMetadata> first = tracker.drainCommittableBatch().offsets();
        assertEquals(2L, first.get(CLICKS_0).offset());

        // Close the gap; the next drain should advance past offset 3.
        tracker.markDone(CLICKS_0, 2L);
        Map<TopicPartition, OffsetAndMetadata> second = tracker.drainCommittableBatch().offsets();
        assertEquals(4L, second.get(CLICKS_0).offset());
    }

    @Test
    void drainWithNothingDone_returnsEmpty() {
        OffsetCommitTracker tracker = new OffsetCommitTracker();
        tracker.register(CLICKS_0, 0L);
        tracker.register(CLICKS_0, 1L);

        assertTrue(tracker.drainCommittableBatch().offsets().isEmpty());
    }

    @Test
    void multiplePartitions_trackedIndependently() {
        OffsetCommitTracker tracker = new OffsetCommitTracker();
        tracker.register(CLICKS_0, 0L);
        tracker.register(CLICKS_1, 5L);
        tracker.register(PV_0, 100L);

        tracker.markDone(CLICKS_0, 0L);
        tracker.markDone(PV_0, 100L);
        // CLICKS_1 left undone

        Map<TopicPartition, OffsetAndMetadata> committable = tracker.drainCommittableBatch().offsets();
        assertEquals(1L, committable.get(CLICKS_0).offset());
        assertEquals(101L, committable.get(PV_0).offset());
        assertEquals(2, committable.size());
    }

    @Test
    void drainConsumesContiguousPrefix_subsequentDrainReturnsOnlyNew() {
        OffsetCommitTracker tracker = new OffsetCommitTracker();
        tracker.register(CLICKS_0, 0L);
        tracker.register(CLICKS_0, 1L);
        tracker.markDone(CLICKS_0, 0L);
        tracker.markDone(CLICKS_0, 1L);

        CommittableOffsets first = tracker.drainCommittableBatch();
        assertEquals(2L, first.offsets().get(CLICKS_0).offset());
        tracker.confirmCommitted(first);
        // No new work — second drain returns empty.
        assertTrue(tracker.drainCommittableBatch().offsets().isEmpty());
    }

    @Test
    void drainBatch_returnsCommitActionForHighestContiguousOffsetPerPartition() {
        OffsetCommitTracker tracker = new OffsetCommitTracker();
        AtomicInteger commits0 = new AtomicInteger();
        AtomicInteger commits1 = new AtomicInteger();
        AtomicInteger commits2 = new AtomicInteger();

        tracker.register(CLICKS_0, 0L, commits0::incrementAndGet);
        tracker.register(CLICKS_0, 1L, commits1::incrementAndGet);
        tracker.register(CLICKS_0, 2L, commits2::incrementAndGet);
        tracker.markDone(CLICKS_0, 0L);
        tracker.markDone(CLICKS_0, 1L);
        // offset 2 still not done, so offset 1 is highest committable record

        CommittableOffsets batch = tracker.drainCommittableBatch();

        assertEquals(2L, batch.offsets().get(CLICKS_0).offset());
        batch.commitActions().get(CLICKS_0).run();
        assertEquals(0, commits0.get());
        assertEquals(1, commits1.get());
        assertEquals(0, commits2.get());
    }

    @Test
    void commitReadyOffsets_failedAcknowledgement_remainsPendingAndIsRetried() {
        OffsetCommitTracker tracker = new OffsetCommitTracker();
        AtomicInteger attempts = new AtomicInteger();
        tracker.register(CLICKS_0, 0L, () -> {
            if (attempts.incrementAndGet() == 1) {
                throw new RuntimeException("injected acknowledgement failure");
            }
        });
        tracker.markDone(CLICKS_0, 0L);

        assertThrows(RuntimeException.class, tracker::commitReadyOffsets);
        tracker.commitReadyOffsets();

        assertEquals(2, attempts.get(), "the same safe prefix must be retried");
    }

}
