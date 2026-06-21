package com.ebay.challenge.streamprocessor.output;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Postgres-backed {@link SinkHealthProbe}. While paused, opens a fresh throwaway
 * connection and runs {@code SELECT 1}.
 *
 * <p>It deliberately does <em>not</em> reuse the per-partition
 * {@link PostgresOutputSink} connections: those are owned by partition worker
 * threads and are not thread-safe, and they are paused mid-transaction. A
 * dedicated short-lived connection on the maintenance thread is the safe way to
 * test DB reachability without touching hot-path state.
 */
@Slf4j
@Component
public class PostgresSinkHealthProbe implements SinkHealthProbe {

    private final BackpressureCoordinator backpressure;
    private final String jdbcUrl;
    private final String user;
    private final String password;

    @Autowired
    public PostgresSinkHealthProbe(
            BackpressureCoordinator backpressure,
            @Value("${postgres.jdbc-url:jdbc:postgresql://postgres:5432/attribution}") String jdbcUrl,
            @Value("${postgres.user:streamprocessor}") String user,
            @Value("${postgres.password:streamprocessor}") String password) {
        this.backpressure = backpressure;
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    @Override
    public void probeIfPaused() {
        if (!backpressure.isPaused()) {
            return;
        }
        try (Connection c = DriverManager.getConnection(jdbcUrl, user, password);
             Statement s = c.createStatement()) {
            s.execute("SELECT 1");
            log.info("Sink health probe succeeded while paused; resuming consumption");
            backpressure.recordSinkSuccess();
        } catch (SQLException e) {
            log.debug("Sink health probe failed; staying paused", e);
        }
    }
}
