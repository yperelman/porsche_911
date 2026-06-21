package com.ebay.challenge.streamprocessor.output;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Graceful-degradation controller for sustained sink failure. The Postgres
 * sink retries each {@code flush()} with bounded exponential backoff; if
 * retries are exhausted, it calls {@link #recordSinkFailure(String)}.
 * After {@code maxConsecutiveFailures} consecutive failures the coordinator
 * <em>pauses</em> all input {@code @KafkaListener} containers via Spring's
 * {@link KafkaListenerEndpointRegistry}. While paused, no new records enter
 * the engine; in-flight buffers stop growing; memory is bounded. A periodic
 * health-check thread can call {@link #recordSinkSuccess()} on the next
 * successful flush, which resumes consumption.
 *
 * <p>Logs pause/resume transitions directly.
 */
@Slf4j
@Component
public class BackpressureCoordinator {

    private final KafkaListenerEndpointRegistry listenerRegistry;
    private final int maxConsecutiveFailures;

    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicBoolean paused = new AtomicBoolean(false);

    public BackpressureCoordinator(
            KafkaListenerEndpointRegistry listenerRegistry,
            @Value("${backpressure.max-consecutive-failures:5}") int maxConsecutiveFailures) {
        this.listenerRegistry = listenerRegistry;
        this.maxConsecutiveFailures = maxConsecutiveFailures;
    }

    /**
     * Called when a sink {@code flush()} fails after exhausting retries.
     * Pauses all listener containers once the threshold is crossed.
     */
    public void recordSinkFailure(String reason) {
        int failures = consecutiveFailures.incrementAndGet();
        if (failures >= maxConsecutiveFailures && paused.compareAndSet(false, true)) {
            log.warn("Pausing all Kafka listeners after {} consecutive sink failures: {}", failures, reason);
            for (MessageListenerContainer container : listenerRegistry.getAllListenerContainers()) {
                container.pause();
            }
        }
    }

    /**
     * Called when a sink {@code flush()} succeeds. Resets the failure counter
     * and resumes listeners if they were paused.
     */
    public void recordSinkSuccess() {
        consecutiveFailures.set(0);
        if (paused.compareAndSet(true, false)) {
            log.info("Resuming all Kafka listeners after sink recovery");
            for (MessageListenerContainer container : listenerRegistry.getAllListenerContainers()) {
                container.resume();
            }
        }
    }

    public boolean isPaused() {
        return paused.get();
    }
}
