package com.ebay.challenge.streamprocessor.output;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BackpressureCoordinatorTest {

    @Test
    void pausesListeners_afterMaxConsecutiveFailures() {
        MessageListenerContainer c1 = mock(MessageListenerContainer.class);
        MessageListenerContainer c2 = mock(MessageListenerContainer.class);
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        when(registry.getAllListenerContainers()).thenReturn(List.of(c1, c2));

        BackpressureCoordinator coord = new BackpressureCoordinator(registry, 3);

        // 2 failures: not yet at threshold
        coord.recordSinkFailure("conn refused");
        coord.recordSinkFailure("conn refused");
        verify(c1, never()).pause();
        assertFalse(coord.isPaused());

        // 3rd failure → pause
        coord.recordSinkFailure("conn refused");
        verify(c1, times(1)).pause();
        verify(c2, times(1)).pause();
        assertTrue(coord.isPaused());

        // Further failures don't re-pause (idempotent)
        coord.recordSinkFailure("conn refused");
        verify(c1, times(1)).pause();
    }

    @Test
    void resumesListeners_onFirstSuccessAfterPause() {
        MessageListenerContainer c1 = mock(MessageListenerContainer.class);
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        when(registry.getAllListenerContainers()).thenReturn(List.of(c1));

        BackpressureCoordinator coord = new BackpressureCoordinator(registry, 1);

        coord.recordSinkFailure("down");
        assertTrue(coord.isPaused());

        coord.recordSinkSuccess();
        verify(c1, times(1)).resume();
        assertFalse(coord.isPaused());

        // Another success doesn't re-resume
        coord.recordSinkSuccess();
        verify(c1, times(1)).resume();
    }

    @Test
    void successResetsFailureCounter() {
        MessageListenerContainer c1 = mock(MessageListenerContainer.class);
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        when(registry.getAllListenerContainers()).thenReturn(List.of(c1));
        BackpressureCoordinator coord = new BackpressureCoordinator(registry, 3);

        coord.recordSinkFailure("x");
        coord.recordSinkFailure("x");
        coord.recordSinkSuccess();  // reset counter
        coord.recordSinkFailure("x");
        coord.recordSinkFailure("x");
        // 2 failures since reset; threshold = 3 → not paused
        verify(c1, never()).pause();
        assertFalse(coord.isPaused());
    }
}
