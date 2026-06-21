package com.ebay.challenge.streamprocessor.output;

/**
 * Resolves the {@link OutputSink} for a given Kafka partition.
 *
 * <p>Each partition owns its own sink instance, so a partition's listener
 * thread never shares mutable sink state with another partition's listener
 * thread. This is what lets the sink's {@code write} and {@code flush}
 * implementations skip the {@code synchronized} keyword while remaining
 * correct under the concurrent listener model — the contention is eliminated
 * by partitioning, not by mutual exclusion.
 *
 * <p>Implementations must:
 * <ul>
 *   <li>Be thread-safe for {@link #sinkFor} itself (different partitions can
 *       resolve simultaneously).</li>
 *   <li>Return the <em>same</em> sink for the same partition across calls
 *       (stable per-partition identity).</li>
 *   <li>Be {@link AutoCloseable} so the application can clean up resources
 *       at shutdown.</li>
 * </ul>
 */
public interface OutputSinkFactory extends AutoCloseable {

    OutputSink sinkFor(int partition);

    @Override
    void close();
}
