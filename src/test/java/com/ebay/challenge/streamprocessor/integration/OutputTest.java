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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static com.ebay.challenge.streamprocessor.TestEvents.click;
import static com.ebay.challenge.streamprocessor.TestEvents.pv;

/**
 * Deterministic golden-output test. Feeds the same 6 scenarios that
 * {@code data_generator.py} ships and asserts the exact contents of
 * {@code attributed_page_views} + {@code dead_letter} in Postgres. This is the
 * canonical reviewer-rerunnable correctness check the README's
 * "Determinism + Testability" section asks for.
 *
 * <p>Expected output (driven by the README + data_generator.py expectations):
 * <ul>
 *   <li>pv_1 → campaign_A (normal: click at T+5, pv at T+10)</li>
 *   <li>pv_2 → campaign_B (out-of-order: pv at T+15 arrives first, click at T+12 late but within lateness)</li>
 *   <li>pv_3 → campaign_D (multiple clicks T+20 + T+25, pick latest)</li>
 *   <li>pv_4 → null attribution (click at T+35 was {@code >} 30min before pv at T+70)</li>
 *   <li>pv_5 → null attribution (click for pv_5 was very-late, dead-lettered)</li>
 *   <li>pv_6 → null attribution (no click ever existed)</li>
 * </ul>
 */
@Testcontainers
class OutputTest {

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
        // Rancher Desktop's port forwarder is briefly slow after container readiness.
        for (int i = 0; i < 30; i++) {
            try (Connection ignored = DriverManager.getConnection(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword())) {
                return;
            } catch (Exception e) {
                Thread.sleep(200);
            }
        }
        throw new IllegalStateException("could not connect to Testcontainers Postgres after retries");
    }

    @AfterAll
    static void truncate() throws Exception {
        try (Connection c = DriverManager.getConnection(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement s = c.createStatement()) {
            s.execute("TRUNCATE TABLE attributed_page_views");
        }
    }

    /** Capturing dead-letter publisher — the production impl is a Kafka producer. */
    static class RecordingDlq implements DeadLetterPublisher {
        final List<String> deadLetteredClickIds = new ArrayList<>();
        @Override public void publish(String sourceTopic, int partition, long offset, String payload, String reason) {
            // Extract the click_id from the payload's toString() representation.
            int i = payload.indexOf("clickId=");
            if (i >= 0) {
                int end = payload.indexOf(',', i);
                if (end < 0) end = payload.indexOf(')', i);
                deadLetteredClickIds.add(payload.substring(i + "clickId=".length(), end));
            }
        }
    }

    @Test
    void fixedWorkload_producesExactExpectedRows() throws Exception {
        RecordingDlq dlq = new RecordingDlq();
        try (PostgresOutputSink sink = new PostgresOutputSink(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword(), null)) {
            ClickStateStore clickStore = new ClickStateStore(30);
            // 15-min lateness so the "very late" scenario triggers dead-letter on pv_5's click.
            WatermarkTracker watermarks = new WatermarkTracker(15, "ad_clicks", "page_views");
            OffsetCommitTracker offsets = new OffsetCommitTracker();
            OutputSinkFactory factory = new OutputSinkFactory() {
                @Override public OutputSink sinkFor(int partition) { return sink; }
                @Override public void close() {}
            };
            JoinEngine engine = new JoinEngine(clickStore, watermarks, factory, dlq, offsets, new MetricsCounters(), new RecentEventLog(false, 0), new PartitionBackpressureService(null, "ad_clicks", Long.MAX_VALUE, Long.MAX_VALUE), "ad_clicks", "page_views", java.time.Clock.systemUTC(), 5L, 30L);

            // ----- 6 scenarios in processing order -----
            // User 1: click before page view, within 30-min window
            engine.processClick(click("user_1", 5, "click_1", "campaign_A", 0, 0));
            engine.processPageView(pv("user_1", 10, "pv_1", 0, 0));

            // User 2: page view arrives first, click arrives late (within 15-min lateness)
            engine.processPageView(pv("user_2", 15, "pv_2", 0, 1));
            engine.processClick(click("user_2", 12, "click_2", "campaign_B", 0, 1));

            // User 3: two clicks then page view → pick the latest click
            engine.processClick(click("user_3", 20, "click_3a", "campaign_C", 0, 2));
            engine.processClick(click("user_3", 25, "click_3b", "campaign_D", 0, 3));
            engine.processPageView(pv("user_3", 30, "pv_3", 0, 2));

            // User 4: click then page view 35 min later — outside the 30-min attribution window
            engine.processClick(click("user_4", 35, "click_4", "campaign_E", 0, 4));
            engine.processPageView(pv("user_4", 70, "pv_4", 0, 3));

            // User 5: page view at T+45; click for user_5 arrives later (T+50 wall-clock would push wm)
            // and is well past T+45 + 15min lateness → dead-letter.
            engine.processPageView(pv("user_5", 45, "pv_5", 0, 4));

            // User 6: page view with no click ever.
            engine.processPageView(pv("user_6", 80, "pv_6", 0, 5));

            // Advance the watermarks on both topics enough to flush every pending PV.
            // After this, joined wm = T+200 → all pv fences (<= T+95) crossed.
            engine.processClick(click("user_advance", 200, "click_advance", "campaign_Z", 0, 5));
            engine.processPageView(pv("user_advance", 200, "pv_advance", 0, 6));

            // Send the very-late click AFTER the watermark has advanced — must be dead-lettered.
            engine.processClick(click("user_5", 40, "click_5", "campaign_F", 0, 6));

            sink.flush();
        }

        // ----- assert exact rows (filter the synthetic watermark-advancer) -----
        Map<String, Row> rows = loadAttributedRows();
        rows.remove("pv_advance");
        Map<String, Row> expected = new LinkedHashMap<>();
        expected.put("pv_1", new Row("pv_1", "user_1", "campaign_A", "click_1"));
        expected.put("pv_2", new Row("pv_2", "user_2", "campaign_B", "click_2"));
        expected.put("pv_3", new Row("pv_3", "user_3", "campaign_D", "click_3b"));
        expected.put("pv_4", new Row("pv_4", "user_4", null, null));
        expected.put("pv_5", new Row("pv_5", "user_5", null, null));
        expected.put("pv_6", new Row("pv_6", "user_6", null, null));
        assertEquals(expected, rows);

        // Dead-letter must contain the very-late click for user_5.
        assertEquals(List.of("click_5"), dlq.deadLetteredClickIds);
    }

    private record Row(String pageViewId, String userId, String campaignId, String clickId) {}

    private static Map<String, Row> loadAttributedRows() throws Exception {
        Map<String, Row> rows = new LinkedHashMap<>();
        try (Connection c = DriverManager.getConnection(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT page_view_id, user_id, attributed_campaign_id, attributed_click_id " +
                             "FROM attributed_page_views ORDER BY page_view_id")) {
            while (rs.next()) {
                rows.put(rs.getString(1), new Row(
                        rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)));
            }
        }
        return rows;
    }
}
