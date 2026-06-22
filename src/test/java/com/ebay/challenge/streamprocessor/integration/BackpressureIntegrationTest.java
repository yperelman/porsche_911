package com.ebay.challenge.streamprocessor.integration;

import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.output.BackpressureCoordinator;
import com.ebay.challenge.streamprocessor.output.OutputSink;
import com.ebay.challenge.streamprocessor.output.OutputSinkFactory;
import com.ebay.challenge.streamprocessor.output.PostgresOutputSink;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end backpressure proof against real Kafka + real Postgres.
 *
 * <p>Wires a {@link ProgrammableSinkFactory} that wraps the real Postgres
 * sink and can be programmatically flipped to fail every {@code flush()}.
 * Sends events through Kafka with monotonically increasing event_time so
 * each phase's events make it past the watermark cutoff.
 *
 * <p>The recovery phase manually resumes listener containers because this test
 * injects failures in the wrapper sink, not in the real Postgres database. The
 * production {@code PostgresSinkHealthProbe} checks the database itself, so it
 * would not observe the synthetic wrapper failure used here.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(BackpressureIntegrationTest.TestSinkConfig.class)
@Testcontainers
class BackpressureIntegrationTest {

    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withUsername("streamprocessor")
            .withPassword("streamprocessor")
            .withDatabaseName("attribution")
            .waitingFor(Wait.forSuccessfulCommand("pg_isready -U streamprocessor -d attribution"))
            .withStartupTimeout(Duration.ofSeconds(60));

    @Container
    @SuppressWarnings("resource")
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @DynamicPropertySource
    static void registerProps(DynamicPropertyRegistry registry) {
        registry.add("kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("postgres.jdbc-url", () -> POSTGRES.getJdbcUrl().replace("localhost", "127.0.0.1"));
        registry.add("postgres.user", POSTGRES::getUsername);
        registry.add("postgres.password", POSTGRES::getPassword);
        // Trip the breaker after 2 failures to keep the test snappy.
        registry.add("backpressure.max-consecutive-failures", () -> "2");
        // Generous lateness so monotonically-increasing event_times across phases stay in-window.
        registry.add("watermark.allowed-lateness-minutes", () -> "15");
    }

    @BeforeAll
    static void initSchema() throws Exception {
        String url = POSTGRES.getJdbcUrl().replace("localhost", "127.0.0.1");
        for (int i = 0; i < 30; i++) {
            try (Connection ignored = DriverManager.getConnection(url, POSTGRES.getUsername(), POSTGRES.getPassword())) {
                break;
            } catch (SQLException e) {
                Thread.sleep(200);
            }
        }
        try (var ignored = new PostgresOutputSink(url, POSTGRES.getUsername(), POSTGRES.getPassword(), null)) {
            // schema is bootstrapped by the constructor
        }
    }

    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired BackpressureCoordinator coordinator;
    @Autowired KafkaListenerEndpointRegistry registry;
    @Autowired ProgrammableSinkFactory programmableFactory;

    /** Hours past 2024-01-01T12:00:00, incremented per phase so each phase outruns the watermark. */
    private final AtomicLong phaseHour = new AtomicLong(0);

    @Test
    void sustainedSinkFailure_pausesListeners_andResumeOnExternalRecovery() {
        // ---- Phase 1: healthy ----
        sendPhase("p1");
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                assertFalse(coordinator.isPaused(), "should still be running healthy"));

        // ---- Phase 2: sink fails. Send 3 phases of events so flushes happen and accumulate ----
        programmableFactory.failuresEnabled.set(true);
        sendPhase("p2");
        sendPhase("p3");
        sendPhase("p4");

        // After ≥2 consecutive failures the coordinator pauses.
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(coordinator.isPaused(), "coordinator must pause after sustained failure");
        });

        // ---- Phase 3: recover ----
        programmableFactory.failuresEnabled.set(false);
        // Synthetic failure is now disabled. Resume listeners so the next real flush can run;
        // that successful flush clears the paused flag.
        registry.getAllListenerContainers().forEach(MessageListenerContainer::resume);

        sendPhase("p5");
        sendPhase("p6");

        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            assertFalse(coordinator.isPaused(), "coordinator must resume on first success after recovery");
        });
    }

    /** Send one click + matching pv + advancer pair per partition with monotonically advancing event_time. */
    private void sendPhase(String tag) {
        long hour = phaseHour.addAndGet(1);  // hours past 2024-01-01 12:00
        String eventTime = LocalDateTime.of(2024, 1, 1, 12, 0)
                .plusHours(hour)
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        String advancerTime = LocalDateTime.of(2024, 1, 1, 12, 0)
                .plusHours(hour).plusMinutes(30)
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

        produce("ad_clicks", "u_" + tag,
                clickJson("u_" + tag, eventTime, "campaign_" + tag, "click_" + tag));
        produce("page_views", "u_" + tag,
                pvJson("u_" + tag, eventTime, "https://example.com/" + tag, "pv_" + tag));
        // Advancer pair on each partition to push the joined watermark past the pv's fence.
        for (int p = 0; p < 3; p++) {
            String key = "_adv_" + tag + "_" + p;
            produce("ad_clicks", p, key, clickJson(key, advancerTime, "_ADV", "_adv_c_" + tag + "_" + p));
            produce("page_views", p, key, pvJson(key, advancerTime, "/_adv", "_adv_pv_" + tag + "_" + p));
        }
    }

    private void produce(String topic, String key, String value) {
        kafkaTemplate.send(topic, key, value);
    }

    private void produce(String topic, int partition, String key, String value) {
        kafkaTemplate.send(topic, partition, key, value);
    }

    /** Matches the AdClickEvent JSON schema exactly (no extra fields — Jackson rejects unknown). */
    private static String clickJson(String userId, String eventTime, String campaignId, String clickId) {
        return String.format(
                "{\"user_id\":\"%s\",\"event_time\":\"%s\",\"campaign_id\":\"%s\",\"click_id\":\"%s\"}",
                userId, eventTime, campaignId, clickId);
    }

    /** Matches the PageViewEvent JSON schema exactly. */
    private static String pvJson(String userId, String eventTime, String url, String eventId) {
        return String.format(
                "{\"user_id\":\"%s\",\"event_time\":\"%s\",\"url\":\"%s\",\"event_id\":\"%s\"}",
                userId, eventTime, url, eventId);
    }

    // ---------------------------------------------------------------- //
    //                       Test wiring                                 //
    // ---------------------------------------------------------------- //

    @TestConfiguration
    static class TestSinkConfig {

        @Bean
        @Primary
        public ProgrammableSinkFactory programmableSinkFactory(
                BackpressureCoordinator coordinator,
                org.springframework.core.env.Environment env) throws SQLException {
            String url = env.getProperty("postgres.jdbc-url");
            String user = env.getProperty("postgres.user");
            String pass = env.getProperty("postgres.password");
            PostgresOutputSink underlying = new PostgresOutputSink(url, user, pass, coordinator);
            return new ProgrammableSinkFactory(underlying, coordinator);
        }
    }

    /** Single-instance sink factory whose wrapped sink can be flipped to fail every flush. */
    static class ProgrammableSinkFactory implements OutputSinkFactory {
        final AtomicBoolean failuresEnabled = new AtomicBoolean(false);
        final AtomicInteger flushCalls = new AtomicInteger();
        private final PostgresOutputSink underlying;
        private final BackpressureCoordinator coordinator;
        private final OutputSink wrapped;

        ProgrammableSinkFactory(PostgresOutputSink underlying, BackpressureCoordinator coordinator) {
            this.underlying = underlying;
            this.coordinator = coordinator;
            this.wrapped = new OutputSink() {
                @Override public boolean write(AttributedPageView record) { return underlying.write(record); }
                @Override public int updateClick(String userId, java.time.Instant clickTime, String clickId,
                                                 String campaignId, java.time.Duration window) {
                    return underlying.updateClick(userId, clickTime, clickId, campaignId, window);
                }
                @Override public void flush() {
                    flushCalls.incrementAndGet();
                    if (failuresEnabled.get()) {
                        coordinator.recordSinkFailure("test-injected failure");
                        throw new RuntimeException("flush failure (test-injected)");
                    }
                    underlying.flush();
                }
            };
        }

        @Override public OutputSink sinkFor(int partition) { return wrapped; }
        @Override public void close() {
            try { underlying.close(); } catch (SQLException ignored) {}
        }
    }
}
