package com.ebay.challenge.streamprocessor.integration;
import com.ebay.challenge.streamprocessor.output.PostgresOutputSink;

import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class PostgresOutputSinkTest {

    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withUsername("streamprocessor")
            .withPassword("streamprocessor")
            .withDatabaseName("attribution")
            // Rancher Desktop streams logs late, so the default LogMessage wait strategy
            // can return before Postgres is actually accepting connections. Poll pg_isready
            // inside the container instead.
            .waitingFor(Wait.forSuccessfulCommand("pg_isready -U streamprocessor -d attribution"))
            .withStartupTimeout(Duration.ofSeconds(60));

    private PostgresOutputSink sink;
    private Connection adminConn;

    @BeforeEach
    void setUp() throws SQLException, InterruptedException {
        String url = ipv4(POSTGRES.getJdbcUrl());
        adminConn = connectWithRetry(url);
        sink = new PostgresOutputSink(url, POSTGRES.getUsername(), POSTGRES.getPassword(), null);
    }

    // Rancher Desktop only forwards container ports to host IPv4. The Postgres JDBC
    // driver tries ::1 first when given "localhost" — force 127.0.0.1.
    private static String ipv4(String jdbcUrl) {
        return jdbcUrl.replace("localhost", "127.0.0.1");
    }

    // Rancher Desktop's port forwarder can be slow to wire up after the container
    // reports ready; retry briefly before giving up.
    private static Connection connectWithRetry(String url) throws SQLException, InterruptedException {
        SQLException last = null;
        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                return DriverManager.getConnection(url, POSTGRES.getUsername(), POSTGRES.getPassword());
            } catch (SQLException e) {
                last = e;
                Thread.sleep(200);
            }
        }
        throw last;
    }

    @AfterEach
    void tearDown() throws SQLException {
        sink.close();
        try (Statement s = adminConn.createStatement()) {
            s.execute("TRUNCATE TABLE attributed_page_views");
        }
        adminConn.close();
    }

    @Test
    void write_thenFlush_persistsRow() throws SQLException {
        AttributedPageView row = AttributedPageView.builder()
                .pageViewId("pv_1")
                .userId("user_1")
                .eventTime(Instant.parse("2024-01-01T12:00:00Z"))
                .url("https://example.com/p")
                .attributedCampaignId("campaign_A")
                .attributedClickId("click_1")
                .build();

        sink.write(row);
        sink.flush();

        try (Statement s = adminConn.createStatement();
             ResultSet rs = s.executeQuery("SELECT page_view_id, user_id, attributed_campaign_id, attributed_click_id FROM attributed_page_views")) {
            assertTrue(rs.next());
            assertEquals("pv_1", rs.getString(1));
            assertEquals("user_1", rs.getString(2));
            assertEquals("campaign_A", rs.getString(3));
            assertEquals("click_1", rs.getString(4));
        }
    }

    @Test
    void write_withNullAttribution_persistsNullColumns() throws SQLException {
        AttributedPageView row = AttributedPageView.builder()
                .pageViewId("pv_lonely")
                .userId("user_x")
                .eventTime(Instant.parse("2024-01-01T12:00:00Z"))
                .url("https://example.com/p")
                .build();

        sink.write(row);
        sink.flush();

        try (Statement s = adminConn.createStatement();
             ResultSet rs = s.executeQuery("SELECT attributed_campaign_id, attributed_click_id FROM attributed_page_views WHERE page_view_id = 'pv_lonely'")) {
            assertTrue(rs.next());
            assertNull(rs.getString(1));
            assertNull(rs.getString(2));
        }
    }

    @Test
    void duplicateWrite_isIdempotent_viaUpsertOnPageViewId() throws SQLException {
        AttributedPageView row = AttributedPageView.builder()
                .pageViewId("pv_dup")
                .userId("user_1")
                .eventTime(Instant.parse("2024-01-01T12:00:00Z"))
                .url("https://example.com/p")
                .attributedCampaignId("campaign_A")
                .attributedClickId("click_1")
                .build();

        assertTrue(sink.write(row), "first write inserts a new row");
        sink.flush();
        assertFalse(sink.write(row), "duplicate write is not a new insert");
        sink.flush();

        try (Statement s = adminConn.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM attributed_page_views WHERE page_view_id = 'pv_dup'")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void samePageViewIdDifferentUser_keptAsSeparateRows() throws SQLException {
        // Identity is the composite PK (page_view_id, user_id) — a shared id across users
        // must not collide into one row.
        sink.write(AttributedPageView.builder()
                .pageViewId("pv_x").userId("user_a")
                .eventTime(Instant.parse("2024-01-01T12:00:00Z")).url("u").build());
        sink.write(AttributedPageView.builder()
                .pageViewId("pv_x").userId("user_b")
                .eventTime(Instant.parse("2024-01-01T12:00:00Z")).url("u").build());
        sink.flush();

        assertEquals("2", scalar("SELECT COUNT(*) FROM attributed_page_views WHERE page_view_id = 'pv_x'"));
    }

    @Test
    void updateClick_reattributesRowsInWindow_latestClickWins() throws SQLException {
        // Page view at 12:20 initially attributed to an older click at 12:05.
        sink.write(AttributedPageView.builder()
                .pageViewId("pv_c").userId("user_1")
                .eventTime(Instant.parse("2024-01-01T12:20:00Z"))
                .url("https://example.com/p")
                .attributedCampaignId("campaign_OLD").attributedClickId("click_old")
                .attributedClickTime(Instant.parse("2024-01-01T12:05:00Z"))
                .build());
        sink.flush();

        // Newer click at 12:10 (window [12:10, 12:40] covers the 12:20 page view) → corrects.
        int corrected = sink.updateClick("user_1", Instant.parse("2024-01-01T12:10:00Z"),
                "click_new", "campaign_NEW", Duration.ofMinutes(30));
        sink.flush();
        assertEquals(1, corrected);
        assertEquals("campaign_NEW", scalar("SELECT attributed_campaign_id FROM attributed_page_views WHERE page_view_id = 'pv_c'"));

        // An older click must not override the newer attribution (latest-click-wins).
        int again = sink.updateClick("user_1", Instant.parse("2024-01-01T12:08:00Z"),
                "click_older", "campaign_OLDER", Duration.ofMinutes(30));
        sink.flush();
        assertEquals(0, again);
        assertEquals("campaign_NEW", scalar("SELECT attributed_campaign_id FROM attributed_page_views WHERE page_view_id = 'pv_c'"));
    }

    private String scalar(String sql) throws SQLException {
        try (Statement s = adminConn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    @Test
    void writeBeforeFlush_isNotDurable_visibleOnlyAfterFlush() throws SQLException {
        AttributedPageView row = AttributedPageView.builder()
                .pageViewId("pv_durability")
                .userId("user_1")
                .eventTime(Instant.parse("2024-01-01T12:00:00Z"))
                .url("https://example.com/p")
                .build();

        sink.write(row);
        // No flush yet — admin connection on a different transaction shouldn't see it.
        try (Statement s = adminConn.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM attributed_page_views WHERE page_view_id = 'pv_durability'")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1), "writes are not durable before flush()");
        }

        sink.flush();
        try (Statement s = adminConn.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM attributed_page_views WHERE page_view_id = 'pv_durability'")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }
}
