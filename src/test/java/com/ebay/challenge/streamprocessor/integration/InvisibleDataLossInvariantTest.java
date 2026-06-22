package com.ebay.challenge.streamprocessor.integration;

import com.ebay.challenge.streamprocessor.backpressure.PartitionBackpressureService;
import com.ebay.challenge.streamprocessor.deadletter.DeadLetterPublisher;
import com.ebay.challenge.streamprocessor.engine.JoinEngine;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.observability.MetricsCounters;
import com.ebay.challenge.streamprocessor.observability.RecentEventLog;
import com.ebay.challenge.streamprocessor.offset.OffsetCommitTracker;
import com.ebay.challenge.streamprocessor.output.OutputSink;
import com.ebay.challenge.streamprocessor.output.OutputSinkFactory;
import com.ebay.challenge.streamprocessor.output.PostgresOutputSink;
import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static com.ebay.challenge.streamprocessor.TestEvents.click;
import static com.ebay.challenge.streamprocessor.TestEvents.pv;

/**
 * Structural invariant test for "invisible data loss." Wraps the real
 * {@link PostgresOutputSink} and {@link OffsetCommitTracker} with decorators
 * that record a timeline of operations. After running the standard workload,
 * asserts the global invariant:
 *
 * <p><strong>For every {@code COMMIT(tp, offset)}, there exists a
 * {@code FLUSH_END} before it such that the {@code WRITE} corresponding to
 * the page view at that offset happened before that {@code FLUSH_END}.</strong>
 *
 * <p>An offset commit that races ahead of its dependent write's durability is
 * the silent-data-loss failure mode: in production it manifests only after a
 * crash, when Kafka resumes past offsets whose output rows were never
 * persisted. This test catches it deterministically before deployment.
 */
@Testcontainers
class InvisibleDataLossInvariantTest {

    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withUsername("streamprocessor")
            .withPassword("streamprocessor")
            .withDatabaseName("attribution")
            .waitingFor(Wait.forSuccessfulCommand("pg_isready -U streamprocessor -d attribution"))
            .withStartupTimeout(Duration.ofSeconds(60));

    static String jdbcUrl;

    @BeforeAll
    static void initJdbcUrl() throws Exception {
        jdbcUrl = POSTGRES.getJdbcUrl().replace("localhost", "127.0.0.1");
        for (int i = 0; i < 30; i++) {
            try (Connection ignored = DriverManager.getConnection(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword())) {
                return;
            } catch (Exception e) {
                Thread.sleep(200);
            }
        }
        throw new IllegalStateException("could not connect to Postgres");
    }

    private static final AtomicLong CLOCK = new AtomicLong(0);

    record Event(long seq, String kind, String topic, long offset, String pageViewId) {}

    /** Sink decorator that records WRITE/FLUSH_START/FLUSH_END to the timeline. */
    static class RecordingSink implements OutputSink {
        final PostgresOutputSink delegate;
        final List<Event> timeline;
        RecordingSink(PostgresOutputSink delegate, List<Event> timeline) {
            this.delegate = delegate;
            this.timeline = timeline;
        }
        @Override public boolean write(AttributedPageView record) {
            timeline.add(new Event(CLOCK.incrementAndGet(), "WRITE", null, -1, record.getPageViewId()));
            return delegate.write(record);
        }
        @Override public int updateClick(String userId, java.time.Instant clickTime, String clickId,
                                         String campaignId, java.time.Duration window) {
            int n = delegate.updateClick(userId, clickTime, clickId, campaignId, window);
            timeline.add(new Event(CLOCK.incrementAndGet(), "CORRECT", null, -1, null));
            return n;
        }
        @Override public void flush() {
            timeline.add(new Event(CLOCK.incrementAndGet(), "FLUSH_START", null, -1, null));
            delegate.flush();
            timeline.add(new Event(CLOCK.incrementAndGet(), "FLUSH_END", null, -1, null));
        }
    }

    /** Dead-letter publisher decorator — records DLQ publishes to the timeline. */
    static class RecordingDlq implements DeadLetterPublisher {
        final List<Event> timeline;
        RecordingDlq(List<Event> timeline) { this.timeline = timeline; }
        @Override public void publish(String sourceTopic, int partition, long offset, String payload, String reason) {
            timeline.add(new Event(CLOCK.incrementAndGet(), "DLQ_PUBLISH", sourceTopic, offset, null));
        }
    }

    /** Tracker decorator: timeline gets MARK_DONE and synthetic COMMIT events. */
    static class RecordingTracker extends OffsetCommitTracker {
        final List<Event> timeline;
        RecordingTracker(List<Event> timeline) { this.timeline = timeline; }
        @Override public void markDone(TopicPartition tp, long offset) {
            timeline.add(new Event(CLOCK.incrementAndGet(), "MARK_DONE", tp.topic(), offset, null));
            super.markDone(tp, offset);
        }
        Map<TopicPartition, OffsetAndMetadata> drainAndRecordCommits() {
            Map<TopicPartition, OffsetAndMetadata> committable = drainCommittableBatch().offsets();
            for (var e : committable.entrySet()) {
                // Each commit means: every offset < returned offset is being "committed to Kafka."
                long upToExclusive = e.getValue().offset();
                for (long o = 0; o < upToExclusive; o++) {
                    timeline.add(new Event(CLOCK.incrementAndGet(), "COMMIT", e.getKey().topic(), o, null));
                }
            }
            return committable;
        }
    }

    @Test
    void noOffsetIsCommitted_beforeItsDependentWriteIsFlushed() throws Exception {
        List<Event> timeline = new ArrayList<>();
        // Map (topic, offset) → page_view_id for PV records, so we can later check the WRITE→FLUSH→COMMIT chain.
        Map<String, String> pvOffsetToId = new HashMap<>();

        try (PostgresOutputSink underlying = new PostgresOutputSink(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword(), null)) {
            RecordingSink sink = new RecordingSink(underlying, timeline);
            RecordingTracker offsets = new RecordingTracker(timeline);
            RecordingDlq dlq = new RecordingDlq(timeline);
            ClickStateStore clickStore = new ClickStateStore(30);
            WatermarkTracker watermarks = new WatermarkTracker(15, "ad_clicks", "page_views");
            OutputSinkFactory factory = new OutputSinkFactory() {
                @Override public OutputSink sinkFor(int partition) { return sink; }
                @Override public void close() {}
            };
            JoinEngine engine = new JoinEngine(clickStore, watermarks, factory, dlq, offsets, new MetricsCounters(), new RecentEventLog(false, 0), new PartitionBackpressureService(null, "ad_clicks", Long.MAX_VALUE, Long.MAX_VALUE), "ad_clicks", "page_views", java.time.Clock.systemUTC(), 5L, 30L);

            // Standard golden-style workload, single partition.
            // Register and process consistently.
            int partition = 0;
            long clickOff = 0, pvOff = 0;

            offsets.register(new TopicPartition("ad_clicks", partition), clickOff);
            engine.processClick(click("u1", 5, "c1", "A", partition, clickOff));
            clickOff++;

            for (int i = 0; i < 3; i++) {
                offsets.register(new TopicPartition("page_views", partition), pvOff);
                String pvId = "pv_" + i;
                pvOffsetToId.put("page_views:" + pvOff, pvId);
                engine.processPageView(pv("u1", 10 + i, pvId, partition, pvOff));
                pvOff++;
            }

            // Advance the watermarks past all PVs' lateness fences.
            offsets.register(new TopicPartition("ad_clicks", partition), clickOff);
            engine.processClick(click("u_other", 120, "c_far", "X", partition, clickOff));
            clickOff++;

            offsets.register(new TopicPartition("page_views", partition), pvOff);
            engine.processPageView(pv("u_other", 120, "pv_far", partition, pvOff));

            // "Commit" the offsets the tracker considers safe.
            offsets.drainAndRecordCommits();
        }

        // ----- Invariant assertion -----
        // For every COMMIT(page_views:offset), find the WRITE for the corresponding pv_id
        // and assert it preceded some FLUSH_END that itself preceded the COMMIT.
        for (Event ev : timeline) {
            if (!"COMMIT".equals(ev.kind())) continue;
            if (!"page_views".equals(ev.topic())) continue; // click commits don't depend on a write
            String key = "page_views:" + ev.offset();
            String pvId = pvOffsetToId.get(key);
            if (pvId == null) continue; // a committed offset that wasn't from our PV set (e.g., the "advance" PV)

            // Find WRITE event for pvId before this COMMIT
            long commitSeq = ev.seq();
            long writeSeq = -1;
            for (Event e2 : timeline) {
                if ("WRITE".equals(e2.kind()) && pvId.equals(e2.pageViewId()) && e2.seq() < commitSeq) {
                    writeSeq = e2.seq();
                    break;
                }
            }
            assertFalse(writeSeq < 0,
                    "INVARIANT VIOLATED: COMMIT for " + key + " (page view " + pvId
                            + ") issued without a preceding WRITE in the timeline. "
                            + "Timeline: " + timeline);

            // Find FLUSH_END strictly between writeSeq and commitSeq
            boolean flushFound = false;
            for (Event e2 : timeline) {
                if ("FLUSH_END".equals(e2.kind()) && e2.seq() > writeSeq && e2.seq() < commitSeq) {
                    flushFound = true;
                    break;
                }
            }
            assertFalse(!flushFound,
                    "INVARIANT VIOLATED: COMMIT for " + key + " (" + pvId + ") issued before any "
                            + "FLUSH_END that came after the WRITE. This is silent data loss territory.");
        }
    }
}
