package com.ebay.challenge.streamprocessor.output;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Spring-managed factory of per-partition {@link PostgresOutputSink} instances.
 * Each numeric partition gets its own dedicated {@link java.sql.Connection}
 * and {@link java.sql.PreparedStatement}s, lazily created on first use, owned
 * by the factory for the application lifetime. {@code StreamConsumer}'s
 * per-partition lock serializes both input topics for the same numeric
 * partition before either topic touches the sink.
 *
 * <p>Replaces the previous design where a single sink (a Spring singleton)
 * serialised all listener threads through {@code synchronized} methods —
 * which created a partition-agnostic global lock that contradicted the
 * "no global locks" property the rest of the design embraces.
 */
@Component
public class PostgresOutputSinkFactory implements OutputSinkFactory {

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final BackpressureCoordinator backpressure;
    private final Map<Integer, PostgresOutputSink> sinks = new ConcurrentHashMap<>();

    @Autowired
    public PostgresOutputSinkFactory(
            @Value("${postgres.jdbc-url:jdbc:postgresql://postgres:5432/attribution}") String jdbcUrl,
            @Value("${postgres.user:streamprocessor}") String user,
            @Value("${postgres.password:streamprocessor}") String password,
            BackpressureCoordinator backpressure) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.backpressure = backpressure;
    }

    @Override
    public OutputSink sinkFor(int partition) {
        return sinks.computeIfAbsent(partition, p -> {
            try {
                return new PostgresOutputSink(jdbcUrl, user, password, backpressure);
            } catch (SQLException e) {
                throw new RuntimeException("failed to open Postgres sink for partition " + p, e);
            }
        });
    }

    @PreDestroy
    @Override
    public void close() {
        sinks.values().forEach(s -> {
            try {
                s.close();
            } catch (SQLException ignored) {
            }
        });
        sinks.clear();
    }
}
