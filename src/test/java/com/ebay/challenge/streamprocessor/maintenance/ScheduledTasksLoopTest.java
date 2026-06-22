package com.ebay.challenge.streamprocessor.maintenance;

import com.ebay.challenge.streamprocessor.offset.OffsetCommitTracker;
import com.ebay.challenge.streamprocessor.output.SinkHealthProbe;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class ScheduledTasksLoopTest {

    @Test
    void invokesSinkHealthProbeEachCycle() {
        OffsetCommitTracker offsets = mock(OffsetCommitTracker.class);
        AtomicInteger probeCalls = new AtomicInteger();
        SinkHealthProbe probe = probeCalls::incrementAndGet;

        ScheduledTasksLoop loop = new ScheduledTasksLoop(offsets, probe, 20);
        loop.start();
        try {
            await().atMost(Duration.ofSeconds(2))
                    .until(() -> probeCalls.get() >= 2);
        } finally {
            loop.stop();
        }
        assertTrue(probeCalls.get() >= 2, "probe should be invoked on every maintenance cycle");
    }
}
