package com.ebay.challenge.streamprocessor.integration;
import com.ebay.challenge.streamprocessor.offset.OffsetCommitTracker;
import com.ebay.challenge.streamprocessor.offset.CommittableOffsets;

import com.ebay.challenge.streamprocessor.maintenance.ScheduledTasksLoop;
import com.ebay.challenge.streamprocessor.output.SinkHealthProbe;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the maintenance loop drives {@link OffsetCommitTracker#commitReadyOffsets()}
 * at the configured cadence and commits/confirms committable offsets.
 */
@SpringBootTest(
        classes = {
                ScheduledTasksLoop.class,
                OffsetCommitMaintenanceIntegrationTest.MaintenanceTestConfig.class
        },
        properties = "maintenance.loop.interval-ms=100"
)
class OffsetCommitMaintenanceIntegrationTest {

    @Autowired CountingOffsetCommitTracker tracker;

    @Test
    void maintenanceLoop_runsDrainAtConfiguredInterval() {
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() ->
                assertTrue(tracker.drainCalls.get() >= 2,
                        "expected maintenance loop to run drain at least twice"));
    }

    @Test
    void maintenanceLoop_commitsCommittableOffsets() {
        TopicPartition tp = new TopicPartition("ad_clicks", 0);
        AtomicInteger actionCommitted = new AtomicInteger();
        tracker.register(tp, 0L, actionCommitted::incrementAndGet);
        tracker.markDone(tp, 0L);

        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(1, tracker.confirmCalls.get());
            assertEquals(1L, tracker.lastConfirmedOffsets.get(tp).offset());
            assertEquals(1, actionCommitted.get());
        });
    }

    @TestConfiguration
    static class MaintenanceTestConfig {
        @Bean
        CountingOffsetCommitTracker offsetCommitTracker() {
            return new CountingOffsetCommitTracker();
        }

        @Bean
        SinkHealthProbe sinkHealthProbe() {
            return () -> { }; // no-op: this test exercises the offset-drain cadence, not self-heal
        }
    }

    static class CountingOffsetCommitTracker extends OffsetCommitTracker {
        final AtomicInteger drainCalls = new AtomicInteger();
        final AtomicInteger confirmCalls = new AtomicInteger();
        volatile Map<TopicPartition, OffsetAndMetadata> lastConfirmedOffsets = Map.of();

        @Override
        public CommittableOffsets drainCommittableBatch() {
            drainCalls.incrementAndGet();
            return super.drainCommittableBatch();
        }

        @Override
        public void confirmCommitted(CommittableOffsets committed) {
            confirmCalls.incrementAndGet();
            lastConfirmedOffsets = committed.offsets();
            super.confirmCommitted(committed);
        }
    }
}
