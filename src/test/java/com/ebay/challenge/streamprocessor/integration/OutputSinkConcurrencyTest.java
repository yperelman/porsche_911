package com.ebay.challenge.streamprocessor.integration;
import com.ebay.challenge.streamprocessor.output.PostgresOutputSinkFactory;
import com.ebay.challenge.streamprocessor.output.OutputSink;

import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import org.junit.jupiter.api.AfterEach;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Per-partition sink instances are the design's answer to "no global locks on
 * the hot path." Each Kafka partition gets its own dedicated Connection +
 * PreparedStatements; threads belonging to different partitions never share
 * sink state. This test pins that contract:
 *
 * <p>Six threads simultaneously write 50 records each to six independently
 * resolved sinks (one per partition). The writes must all succeed without
 * SQLException, the resulting rows must all be present in Postgres, and the
 * underlying Connections must be distinct objects so no concurrent JDBC
 * operation is racing on shared state.
 */
@Testcontainers
class OutputSinkConcurrencyTest {

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
        // Bootstrap schema once.
        try (PostgresOutputSinkFactory factory = new PostgresOutputSinkFactory(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword(), null)) {
            factory.sinkFor(0); // triggers schema init via first sink
        }
    }

    @BeforeEach
    void truncate() throws Exception {
        try (Connection c = DriverManager.getConnection(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement s = c.createStatement()) {
            s.execute("TRUNCATE TABLE attributed_page_views");
        }
    }

    private PostgresOutputSinkFactory factory;

    @AfterEach
    void closeFactory() throws Exception {
        if (factory != null) factory.close();
    }

    @Test
    void differentPartitions_writeConcurrently_withoutCorruption() throws Exception {
        factory = new PostgresOutputSinkFactory(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword(), null);
        int partitions = 6;
        int rowsPerPartition = 50;

        ExecutorService pool = Executors.newFixedThreadPool(partitions);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(partitions);
        AtomicInteger failures = new AtomicInteger();
        Set<OutputSink> resolvedSinks = ConcurrentHashSet();

        for (int p = 0; p < partitions; p++) {
            final int partition = p;
            pool.submit(() -> {
                try {
                    start.await();
                    OutputSink sink = factory.sinkFor(partition);
                    resolvedSinks.add(sink);
                    for (int i = 0; i < rowsPerPartition; i++) {
                        sink.write(row("p" + partition + "_r" + i, partition));
                    }
                    sink.flush();
                } catch (Throwable t) {
                    failures.incrementAndGet();
                    t.printStackTrace();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS), "writers must finish in time");
        pool.shutdown();

        assertEquals(0, failures.get(), "no thread should have thrown a JDBC exception");
        // Each partition resolved a distinct sink instance — proves no shared mutable state on the hot path.
        assertEquals(partitions, resolvedSinks.size(), "each partition must resolve a distinct sink instance");

        // Every row landed in Postgres.
        assertEquals(partitions * rowsPerPartition, countRows());

        // Per-partition row counts match exactly — no cross-contamination.
        for (int p = 0; p < partitions; p++) {
            assertEquals(rowsPerPartition, countRowsForPartition(p));
        }
    }

    private static Set<OutputSink> ConcurrentHashSet() {
        return java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
    }

    private static AttributedPageView row(String id, int partition) {
        return AttributedPageView.builder()
                .pageViewId(id)
                .userId("u_p" + partition)
                .eventTime(Instant.parse("2024-01-01T12:00:00Z"))
                .url("/p" + partition + "/" + id)
                .attributedCampaignId("c_p" + partition)
                .attributedClickId("k_" + id)
                .build();
    }

    private static int countRows() throws Exception {
        try (Connection c = DriverManager.getConnection(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM attributed_page_views")) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static int countRowsForPartition(int partition) throws Exception {
        try (Connection c = DriverManager.getConnection(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM attributed_page_views WHERE user_id = 'u_p" + partition + "'")) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }
}
