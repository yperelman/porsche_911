package com.ebay.challenge.streamprocessor.output;

/**
 * Resume mechanism for the {@link BackpressureCoordinator} pause state.
 *
 * <p>When sustained sink failures pause the Kafka listeners, no records flow,
 * so no {@code flush()} happens — which means {@link BackpressureCoordinator#recordSinkSuccess()}
 * (the only resume trigger) would never fire on its own. That is a self-inflicted
 * deadlock: the system stays paused forever even after the sink recovers.
 *
 * <p>A {@code SinkHealthProbe} is driven externally (by the maintenance loop) and,
 * while paused, performs a lightweight out-of-band health check. On success it
 * resumes consumption via the coordinator.
 */
public interface SinkHealthProbe {

    /**
     * No-op when the coordinator is not paused. When paused, runs a lightweight
     * health check and, on success, calls {@link BackpressureCoordinator#recordSinkSuccess()}
     * to resume the listeners. Never throws — a failed probe simply leaves the
     * system paused until the next attempt.
     */
    void probeIfPaused();
}
