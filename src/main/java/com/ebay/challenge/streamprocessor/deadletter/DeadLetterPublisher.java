package com.ebay.challenge.streamprocessor.deadletter;

/**
 * Publishes events that arrived past the allowed-lateness fence to a
 * dedicated Kafka topic so they can be inspected, audited, or replayed
 * separately from the main attribution stream.
 *
 * <p>Implementations are expected to return only after the underlying
 * publish is durably acknowledged (e.g., Kafka {@code acks=all}). The
 * {@link com.ebay.challenge.streamprocessor.engine.JoinEngine} treats this
 * as a synchronous durability barrier: only after {@code publish} returns
 * is the corresponding source offset commit-safe.
 */
public interface DeadLetterPublisher {

    /**
     * Publish a dead-letter envelope for an event that could not be
     * processed normally.
     *
     * @param sourceTopic the topic the original event came from
     * @param partition   the partition the original event came from
     * @param offset      the offset the original event was at
     * @param payload     the original event payload (typically JSON or
     *                    the model's {@code toString()})
     * @param reason      human-readable cause (e.g., "event_time before
     *                    watermark cutoff")
     */
    void publish(String sourceTopic, int partition, long offset, String payload, String reason);
}
