package com.ebay.challenge.streamprocessor.output;

import com.ebay.challenge.streamprocessor.model.AttributedPageView;

import java.time.Duration;
import java.time.Instant;

/**
 * Output contract for attributed page views.
 *
 * Implementations distinguish two phases of durability:
 * <ul>
 *   <li>{@link #write} appends a record; may buffer in memory or an open
 *       transaction. Not guaranteed durable after return.</li>
 *   <li>{@link #flush} forces a durability barrier. Every {@code write}
 *       called before {@code flush} returns is durable on disk after
 *       {@code flush} returns successfully.</li>
 * </ul>
 *
 * Kafka offset commits must wait until the corresponding records' work is
 * flushed; the {@link com.ebay.challenge.streamprocessor.offset.OffsetCommitTracker}
 * marks offsets done only after a successful flush.
 *
 * <p>Dead-letter output is a separate concern handled by
 * {@link com.ebay.challenge.streamprocessor.deadletter.DeadLetterPublisher},
 * which publishes to a dedicated Kafka topic with its own synchronous
 * durability semantics (independent of this sink's flush barrier).
 */
public interface OutputSink {

    /**
     * UPSERT a page-view row by its (page_view_id, user_id) primary key.
     *
     * @return true iff this call inserted a new row; false if it was a duplicate
     *         (conflict update or a guard-skipped no-op). Lets callers count metrics
     *         once per distinct page view rather than once per (re)delivery.
     */
    boolean write(AttributedPageView record);

    /**
     * Re-attribute already-written page views affected by a newly arrived click:
     * all rows for {@code userId} whose {@code event_time} lies in
     * {@code [clickTime, clickTime + window]}, but only where this click is newer
     * than the row's current attribution (latest-click-wins). Appends to the open
     * transaction; the caller must {@link #flush()} to make it durable.
     *
     * @return the number of rows changed (0 means nothing to flush)
     */
    int updateClick(String userId, Instant clickTime, String clickId, String campaignId, Duration window);

    void flush();
}
