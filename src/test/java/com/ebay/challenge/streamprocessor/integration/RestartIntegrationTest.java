package com.ebay.challenge.streamprocessor.integration;

import com.ebay.challenge.streamprocessor.backpressure.PartitionBackpressureService;
import com.ebay.challenge.streamprocessor.deadletter.DeadLetterPublisher;
import com.ebay.challenge.streamprocessor.engine.JoinEngine;
import com.ebay.challenge.streamprocessor.observability.MetricsCounters;
import com.ebay.challenge.streamprocessor.observability.RecentEventLog;
import com.ebay.challenge.streamprocessor.offset.OffsetCommitTracker;
import com.ebay.challenge.streamprocessor.output.OutputSink;
import com.ebay.challenge.streamprocessor.output.OutputSinkFactory;
import com.ebay.challenge.streamprocessor.output.PostgresOutputSink;
import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static com.ebay.challenge.streamprocessor.TestEvents.attributed;
import static com.ebay.challenge.streamprocessor.TestEvents.click;
import static com.ebay.challenge.streamprocessor.TestEvents.pv;

/**
 * Restart safety tests. The processor's restart story is "at-least-once
 * delivery + idempotent sink → effectively exactly-once for output." These
 * tests prove that property directly against real Postgres.
 */
@Testcontainers
class RestartIntegrationTest {

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
                break;
            } catch (Exception e) {
                Thread.sleep(200);
            }
        }
        // Schema is bootstrapped by the sink constructor — instantiate once so the tables exist.
        try (PostgresOutputSink ignored = new PostgresOutputSink(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword(), null)) {
        }
    }

    @BeforeEach
    void truncate() throws Exception {
        try (Connection c = DriverManager.getConnection(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement s = c.createStatement()) {
            s.execute("TRUNCATE TABLE attributed_page_views");
        }
    }

    /** No-op dead-letter publisher — restart tests don't exercise late events. */
    private static final DeadLetterPublisher NOOP_DLQ = (t, p, o, payload, reason) -> {};

    /** Drives a partition's joined watermark far enough to flush every PV at <= eventTimeMin = 80. */
    private void runGoldenWorkload(JoinEngine engine) {
        engine.processClick(click("u1", 5, "c1", "A", 0, 0));
        engine.processPageView(pv("u1", 10, "pv_1", 0, 0));
        engine.processPageView(pv("u2", 15, "pv_2", 0, 1));
        engine.processClick(click("u2", 12, "c2", "B", 0, 1));
        engine.processClick(click("u3", 20, "c3a", "C", 0, 2));
        engine.processClick(click("u3", 25, "c3b", "D", 0, 3));
        engine.processPageView(pv("u3", 30, "pv_3", 0, 2));
        engine.processClick(click("u4", 35, "c4", "E", 0, 4));
        engine.processPageView(pv("u4", 70, "pv_4", 0, 3));
        engine.processPageView(pv("u6", 80, "pv_6", 0, 4));
        // Advance watermarks past the last fence.
        engine.processClick(click("u_a", 200, "c_a", "Z", 0, 5));
        engine.processPageView(pv("u_a", 200, "pv_a", 0, 5));
    }

    @Test
    void replayingTheSameEventsTwice_producesNoDuplicateRows() throws Exception {
        // Run 1
        try (PostgresOutputSink sink = new PostgresOutputSink(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword(), null)) {
            JoinEngine engine = newEngine(sink);
            runGoldenWorkload(engine);
            sink.flush();
        }
        // Run 2 (simulating replay after restart)
        try (PostgresOutputSink sink = new PostgresOutputSink(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword(), null)) {
            JoinEngine engine = newEngine(sink);
            runGoldenWorkload(engine);
            sink.flush();
        }

        // Each page_view_id appears at most once thanks to UPSERT.
        Map<String, Integer> counts = countByPageViewId();
        counts.remove("pv_a"); // synthetic watermark advancer
        assertEquals(Map.of("pv_1", 1, "pv_2", 1, "pv_3", 1, "pv_4", 1, "pv_6", 1), counts);
    }

    @Test
    void crashBetweenWriteAndFlush_losesUnflushedRows_butKeepsTheLastSuccessfulFlush() throws Exception {
        // Phase 1: write a record then flush — this row IS durable.
        try (PostgresOutputSink sink = new PostgresOutputSink(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword(), null)) {
            sink.write(attributed("pv_committed", "campaign_OK"));
            sink.flush();
        }

        // Phase 2: open a new sink, write a record, but "crash" before flush.
        PostgresOutputSink crashSink = new PostgresOutputSink(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword(), null);
        crashSink.write(attributed("pv_lost", "campaign_GHOST"));
        // Crash simulation: close without flush. close() rolls back the open transaction.
        crashSink.close();

        // Phase 3: from a fresh sink, the committed row is present and the unflushed row is gone.
        // This is the "at-least-once + idempotent ⇒ no data loss" property: on real replay the
        // upstream Kafka offsets for pv_lost would still be uncommitted, so the consumer would
        // re-deliver it and the sink would write it again.
        Map<String, Integer> rows = countByPageViewId();
        assertEquals(Map.of("pv_committed", 1), rows,
                "unflushed writes must be absent after a crash; flushed writes must persist");
    }

    @Test
    void replayingAfterCrash_recoversTheLostRow_viaUpsertIdempotency() throws Exception {
        // Phase 1: write + flush a baseline row.
        try (PostgresOutputSink sink = new PostgresOutputSink(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword(), null)) {
            sink.write(attributed("pv_pre", "campaign_OK"));
            sink.flush();
        }

        // Phase 2: simulate processing two more rows; flush only completes on the SECOND.
        // After the first row's "crash", on replay the engine would re-emit both,
        // but UPSERT means the post-replay state is identical.
        try (PostgresOutputSink sink = new PostgresOutputSink(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword(), null)) {
            sink.write(attributed("pv_a", "campaign_A"));
            // (crash happens here in production — Kafka offsets for pv_a aren't committed yet)
            // close without flush drops it.
        }

        // Phase 3: replay — the consumer re-delivers pv_a, sink writes it again, flushes successfully.
        try (PostgresOutputSink sink = new PostgresOutputSink(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword(), null)) {
            sink.write(attributed("pv_a", "campaign_A"));
            sink.write(attributed("pv_b", "campaign_B"));
            sink.flush();
        }

        Map<String, Integer> rows = countByPageViewId();
        // pv_pre, pv_a, pv_b — each exactly once.
        assertEquals(Map.of("pv_pre", 1, "pv_a", 1, "pv_b", 1), rows);
    }

    private JoinEngine newEngine(PostgresOutputSink sink) {
        OutputSinkFactory factory = new OutputSinkFactory() {
            @Override public OutputSink sinkFor(int partition) { return sink; }
            @Override public void close() {}
        };
        return new JoinEngine(
                new ClickStateStore(30),
                new WatermarkTracker(15, "ad_clicks", "page_views"),
                factory,
                NOOP_DLQ,
                new OffsetCommitTracker(),
                new MetricsCounters(),
                new RecentEventLog(false, 0),
                new PartitionBackpressureService(null, "ad_clicks", Long.MAX_VALUE, Long.MAX_VALUE),
                "ad_clicks", "page_views", java.time.Clock.systemUTC(), 5L, 30L);
    }

    private Map<String, Integer> countByPageViewId() throws Exception {
        Map<String, Integer> counts = new HashMap<>();
        try (Connection c = DriverManager.getConnection(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT page_view_id, COUNT(*) FROM attributed_page_views GROUP BY page_view_id")) {
            while (rs.next()) {
                counts.put(rs.getString(1), rs.getInt(2));
            }
        }
        return counts;
    }
}
