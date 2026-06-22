package com.ebay.challenge.streamprocessor.output;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PostgresSinkHealthProbeTest {

    private static BackpressureCoordinator pausedCoordinator(MessageListenerContainer container) {
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        when(registry.getAllListenerContainers()).thenReturn(List.of(container));
        BackpressureCoordinator coord = new BackpressureCoordinator(registry, 1);
        coord.recordSinkFailure("down"); // threshold 1 → paused
        assertTrue(coord.isPaused());
        return coord;
    }

    @Test
    void staysPaused_whenProbeFails() {
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        BackpressureCoordinator coord = pausedCoordinator(container);

        SinkHealthProbe probe = new PostgresSinkHealthProbe(coord,
                "jdbc:postgresql://localhost:1/nonexistent", "u", "p");
        probe.probeIfPaused();

        assertTrue(coord.isPaused(), "failed probe must leave the system paused");
    }

    @Test
    void doesNothing_whenNotPaused() throws Exception {
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        when(registry.getAllListenerContainers()).thenReturn(List.of());
        BackpressureCoordinator coord = new BackpressureCoordinator(registry, 1);
        assertFalse(coord.isPaused());

        SinkHealthProbe probe = new PostgresSinkHealthProbe(coord, "jdbc:postgresql://localhost:1/nonexistent", "u", "p");

        probe.probeIfPaused();

        assertFalse(coord.isPaused());
    }

    /**
     * The success path (probe connects, coordinator resumes) is covered by
     * {@link com.ebay.challenge.streamprocessor.BackpressureIntegrationTest}.
     * Unit-testing it here would require injecting a mock Connection, which we
     * intentionally removed from production code — the real connection is
     * driven by {@code DriverManager.getConnection} directly.
     */
}
