package com.ebay.challenge.streamprocessor.output;

import com.ebay.challenge.streamprocessor.model.AttributedPageView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.Collectors;

/**
 * JDBC-backed sink writing into Postgres on a single dedicated connection.
 * The connection runs in manual-commit mode: {@link #write} appends inside an
 * open transaction; {@link #flush} calls {@code commit()} which forces server-
 * side WAL durability before returning.
 *
 * <p><strong>Thread-safety:</strong> a single instance is <em>not</em> safe for
 * concurrent use — JDBC {@link java.sql.Connection} and
 * {@link java.sql.PreparedStatement} are not thread-safe. The application
     * model is one sink instance per numeric Kafka partition (see
     * {@link OutputSinkFactory}). {@code StreamConsumer}'s per-partition lock
     * serializes both input topics for the same numeric partition before they
     * reach this sink, so cross-thread concurrency on a single sink does not
     * occur on the hot path.
 *
 * <p>Schema is initialised from {@code schema.sql} at construction time.
 */
public class PostgresOutputSink implements OutputSink, AutoCloseable {

    private static final String UPSERT_PV = """
            INSERT INTO attributed_page_views
                (page_view_id, user_id, event_time, url,
                 attributed_campaign_id, attributed_click_id, attributed_click_time)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (page_view_id, user_id) DO UPDATE SET
                event_time = EXCLUDED.event_time,
                url = EXCLUDED.url,
                attributed_campaign_id = EXCLUDED.attributed_campaign_id,
                attributed_click_id = EXCLUDED.attributed_click_id,
                attributed_click_time = EXCLUDED.attributed_click_time
            WHERE attributed_page_views.attributed_click_time IS NULL
               OR (EXCLUDED.attributed_click_time IS NOT NULL
                   AND (EXCLUDED.attributed_click_time, EXCLUDED.attributed_click_id)
                       >= (attributed_page_views.attributed_click_time, attributed_page_views.attributed_click_id))
            RETURNING (xmax = 0) AS inserted
            """;

    // Re-attribute a user's rows in [click_time, click_time + window], latest-click-wins.
    // Lower bound (event_time >= click_time): a click can only attribute a page view that
    // happened at or after it. Guard: apply only if this click is strictly newer than the
    // row's current attribution, tie-breaking equal click times by click_id — matching the
    // (event_time, click_id) ordering used by ClickStateStore.
    private static final String CORRECT_FOR_CLICK = """
            UPDATE attributed_page_views
               SET attributed_campaign_id = ?, attributed_click_id = ?, attributed_click_time = ?
             WHERE user_id = ?
               AND event_time >= ?
               AND event_time <= ?
               AND (attributed_click_time IS NULL
                    OR attributed_click_time < ?
                    OR (attributed_click_time = ? AND attributed_click_id < ?))
            """;

    private final Connection conn;
    private final PreparedStatement upsertStmt;
    private final PreparedStatement correctStmt;
    private final BackpressureCoordinator backpressure;

    public PostgresOutputSink(String jdbcUrl, String user, String password,
                              BackpressureCoordinator backpressure) throws SQLException {
        this.conn = DriverManager.getConnection(jdbcUrl, user, password);
        this.conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.execute("SET idle_in_transaction_session_timeout = 10000");
        }
        conn.commit();
        initSchema();
        this.upsertStmt = conn.prepareStatement(UPSERT_PV);
        this.correctStmt = conn.prepareStatement(CORRECT_FOR_CLICK);
        this.backpressure = backpressure;
    }

    private void initSchema() throws SQLException {
        String schema = readClasspath("/schema.sql");
        try (Statement s = conn.createStatement()) {
            s.execute(schema);
        }
        conn.commit();
    }

    private static String readClasspath(String resource) {
        try (InputStream in = PostgresOutputSink.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("missing classpath resource: " + resource);
            }
            try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                return r.lines().collect(Collectors.joining("\n"));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean write(AttributedPageView record) {
        try {
            upsertStmt.setString(1, record.getPageViewId());
            upsertStmt.setString(2, record.getUserId());
            upsertStmt.setTimestamp(3, Timestamp.from(record.getEventTime()));
            upsertStmt.setString(4, record.getUrl());
            setNullableString(upsertStmt, 5, record.getAttributedCampaignId());
            setNullableString(upsertStmt, 6, record.getAttributedClickId());
            setNullableTimestamp(upsertStmt, 7, record.getAttributedClickTime());
            // RETURNING (xmax = 0): true on a fresh insert, false on a conflict update;
            // a guard-skipped duplicate returns no row at all.
            try (ResultSet rs = upsertStmt.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("write failed", e);
        }
    }

    @Override
    public int updateClick(String userId, Instant clickTime, String clickId,
                           String campaignId, Duration window) {
        try {
            Timestamp ct = Timestamp.from(clickTime);
            setNullableString(correctStmt, 1, campaignId);
            correctStmt.setString(2, clickId);
            correctStmt.setTimestamp(3, ct);
            correctStmt.setString(4, userId);
            correctStmt.setTimestamp(5, ct);                              // event_time >= click_time
            correctStmt.setTimestamp(6, Timestamp.from(clickTime.plus(window))); // event_time <= click_time + window
            correctStmt.setTimestamp(7, ct);                             // attributed_click_time < click_time
            correctStmt.setTimestamp(8, ct);                             // tie: attributed_click_time = click_time
            correctStmt.setString(9, clickId);                          // tie: attributed_click_id < click_id
            return correctStmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("correction failed", e);
        }
    }

    @Override
    public void flush() {
        // Retry the commit with bounded exponential backoff to absorb transient
        // network blips. After exhausting attempts, throw — and notify the
        // BackpressureCoordinator so it can pause listener containers if this
        // becomes a sustained failure.
        try {
            retryWithBackoff(conn::commit, MAX_FLUSH_ATTEMPTS, INITIAL_BACKOFF_MS, MAX_BACKOFF_MS);
            if (backpressure != null) backpressure.recordSinkSuccess();
        } catch (SQLException e) {
            if (backpressure != null) backpressure.recordSinkFailure(e.getMessage());
            throw new RuntimeException("flush failed after retries", e);
        }
    }

    static final int MAX_FLUSH_ATTEMPTS = 5;
    static final long INITIAL_BACKOFF_MS = 100;
    static final long MAX_BACKOFF_MS = 5_000;

    @FunctionalInterface
    interface SqlOp { void run() throws SQLException; }

    /** Visible-for-test: execute {@code op} with bounded exponential backoff. */
    static void retryWithBackoff(SqlOp op, int maxAttempts, long initialDelayMs, long maxDelayMs) throws SQLException {
        SQLException last = null;
        long delayMs = initialDelayMs;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                op.run();
                return;
            } catch (SQLException e) {
                last = e;
                if (attempt == maxAttempts) break;
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    SQLException wrapper = new SQLException("retry interrupted", ie);
                    wrapper.addSuppressed(last);
                    throw wrapper;
                }
                delayMs = Math.min(delayMs * 2, maxDelayMs);
            }
        }
        throw last;
    }

    @Override
    public void close() throws SQLException {
        try {
            try { upsertStmt.close(); } catch (SQLException ignored) {}
            try { correctStmt.close(); } catch (SQLException ignored) {}
            try { conn.rollback(); } catch (SQLException ignored) {}
        } finally {
            conn.close();
        }
    }

    private static void setNullableString(PreparedStatement stmt, int index, String value) throws SQLException {
        if (value == null) {
            stmt.setNull(index, Types.VARCHAR);
        } else {
            stmt.setString(index, value);
        }
    }

    private static void setNullableTimestamp(PreparedStatement stmt, int index, Instant value) throws SQLException {
        if (value == null) {
            stmt.setNull(index, Types.TIMESTAMP);
        } else {
            stmt.setTimestamp(index, Timestamp.from(value));
        }
    }
}
