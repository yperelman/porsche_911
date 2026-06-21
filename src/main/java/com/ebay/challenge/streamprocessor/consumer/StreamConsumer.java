package com.ebay.challenge.streamprocessor.consumer;

import com.ebay.challenge.streamprocessor.deadletter.DeadLetterPublisher;
import com.ebay.challenge.streamprocessor.engine.JoinEngine;
import com.ebay.challenge.streamprocessor.exception.PartitionTopologyException;
import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.ebay.challenge.streamprocessor.observability.MetricsCounters;
import com.ebay.challenge.streamprocessor.observability.RecentEventLog;
import com.ebay.challenge.streamprocessor.observability.RecentEventLog.EventType;
import com.ebay.challenge.streamprocessor.offset.OffsetCommitTracker;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer entry point for both input streams. Parses the JSON payload,
 * tags the partition + offset on the domain event, registers the offset with
 * {@link OffsetCommitTracker}, then hands the event to {@link JoinEngine}.
 * Offsets are NOT acknowledged here — commits flow through the tracker
 * once their work is durable.
 *
 * <p><strong>Per-partition serialization.</strong> {@code ad_clicks-N} and
 * {@code page_views-N} arrive on two <em>different</em> listener threads
 * (separate Spring containers per topic), yet both mutate the same numeric
 * partition's join state and its non-thread-safe {@link
 * com.ebay.challenge.streamprocessor.output.PostgresOutputSink}. We restore the
 * "single thread per partition" guarantee with a per-partition lock: every
 * record runs under {@code synchronized(partitionLocks[partition])}, so work for
 * the same numeric partition is serialized while different partitions run in
 * parallel. The call blocks the listener thread until the work completes, so
 * exceptions still propagate to Spring Kafka's error handling.
 */
@Slf4j
@Component
public class StreamConsumer {

    private final JoinEngine joinEngine;
    private final OffsetCommitTracker offsetCommits;
    private final ObjectMapper objectMapper;
    private final MetricsCounters metrics;
    private final RecentEventLog eventLog;
    private final DeadLetterPublisher deadLetterPublisher;
    private final Object[] partitionLocks;

    public StreamConsumer(
            JoinEngine joinEngine,
            OffsetCommitTracker offsetCommits,
            ObjectMapper objectMapper,
            MetricsCounters metrics,
            RecentEventLog eventLog,
            DeadLetterPublisher deadLetterPublisher,
            @Value("${kafka.consumer.concurrency:3}") int partitionCount) {
        this.joinEngine = joinEngine;
        this.offsetCommits = offsetCommits;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.eventLog = eventLog;
        this.deadLetterPublisher = deadLetterPublisher;
        this.partitionLocks = new Object[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            this.partitionLocks[i] = new Object();
        }
    }

    @KafkaListener(
            topics = "${kafka.topics.ad-clicks:ad_clicks}",
            groupId = "${kafka.consumer.group-id:stream-processor-group}",
            containerFactory = "adClickListenerContainerFactory"
    )
    public void consumeAdClick(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        AdClickEvent click;
        try {
            click = objectMapper.readValue(record.value(), AdClickEvent.class);
        } catch (Exception e) {
            log.error("Failed to parse ad_click at partition {} offset {}: {}",
                    record.partition(), record.offset(), record.value(), e);
            registerInvalidEvent(record, acknowledgment, "ad_click parse failed");
            return;
        }
        registerOffset(record, acknowledgment);
        click.setPartition(record.partition());
        click.setOffset(record.offset());
        metrics.incrementClicksConsumed();
        eventLog.recordConsumed(EventType.AD_CLICK, click.getClickId(), click.getUserId(),
                click.getEventTime(), record.partition(), record.offset());
        runOnPartition(record.partition(), () -> joinEngine.processClick(click));
    }

    @KafkaListener(
            topics = "${kafka.topics.page-views:page_views}",
            groupId = "${kafka.consumer.group-id:stream-processor-group}",
            containerFactory = "pageViewListenerContainerFactory"
    )
    public void consumePageView(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        PageViewEvent pv;
        try {
            pv = objectMapper.readValue(record.value(), PageViewEvent.class);
        } catch (Exception e) {
            log.error("Failed to parse page_view at partition {} offset {}: {}",
                    record.partition(), record.offset(), record.value(), e);
            registerInvalidEvent(record, acknowledgment, "page_view parse failed");
            return;
        }
        registerOffset(record, acknowledgment);
        pv.setPartition(record.partition());
        pv.setOffset(record.offset());
        metrics.incrementPageViewsConsumed();
        eventLog.recordConsumed(EventType.PAGE_VIEW, pv.getEventId(), pv.getUserId(),
                pv.getEventTime(), record.partition(), record.offset());
        runOnPartition(record.partition(), () -> joinEngine.processPageView(pv));
    }

    /**
     * Serializes all join work for the same numeric partition under a dedicated
     * lock, then runs it on the calling listener thread. Fails fast if the
     * record's partition is outside the configured topology — e.g. after an
     * unsafe live partition expansion that would break the {@code user_id ->
     * partition} mapping the join relies on.
     */
    private void runOnPartition(int partition, Runnable work) {
        if (partition < 0 || partition >= partitionLocks.length) {
            throw new PartitionTopologyException(
                    "Kafka topology mismatch: received partition " + partition
                            + " but service was started with " + partitionLocks.length + " partition locks. "
                            + "This processor requires ad_clicks and page_views to have the same fixed "
                            + "partition count, keyed by user_id. Do not expand partitions while state "
                            + "is live; stop the processor, reset or migrate state, update "
                            + "kafka.consumer.concurrency, and restart.");
        }
        synchronized (partitionLocks[partition]) {
            work.run();
        }
    }

    /**
     * Handle an unparseable record: publish the raw payload to the dead-letter topic,
     * then register and immediately mark its offset done so the partition's contiguous
     * commit prefix keeps advancing. Without this a poison record is retried forever and
     * blocks every later offset on the partition.
     */
    private void registerInvalidEvent(ConsumerRecord<String, String> record, Acknowledgment acknowledgment, String reason) {
        deadLetterPublisher.publish(record.topic(), record.partition(), record.offset(), record.value(), reason);
        registerOffset(record, acknowledgment);
        offsetCommits.markDone(new TopicPartition(record.topic(), record.partition()), record.offset());
    }

    private void registerOffset(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        if (acknowledgment == null) {
            offsetCommits.register(tp, record.offset());
        } else {
            offsetCommits.register(tp, record.offset(), acknowledgment::acknowledge);
        }
    }
}
