package com.ebay.challenge.streamprocessor.state;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.event.ListenerContainerPartitionIdleEvent;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

/**
 * Converts Spring Kafka's partition-idle events into safe watermark idleness.
 * Wall-clock silence is not enough: a paused or backlogged partition must keep
 * constraining the join. The event supplies the live consumer, so this listener
 * checks the current position against the broker end offset before marking idle.
 */
@Slf4j
@Component
public class KafkaIdleSourceListener {

    private final WatermarkTracker watermarks;

    public KafkaIdleSourceListener(WatermarkTracker watermarks) {
        this.watermarks = watermarks;
    }

    @EventListener
    public void onPartitionIdle(ListenerContainerPartitionIdleEvent event) {
        TopicPartition tp = event.getTopicPartition();
        Consumer<?, ?> consumer = event.getConsumer();
        boolean partitionDrained = false;
        try {
            long position = consumer.position(tp);
            Map<TopicPartition, Long> ends = consumer.endOffsets(Set.of(tp));
            partitionDrained = position >= ends.getOrDefault(tp, Long.MAX_VALUE);
        } catch (RuntimeException e) {
            log.warn("Could not inspect Kafka position for idle check: {}", tp, e);
        }
        watermarks.markIdleIfCaughtUp(tp.partition(), tp.topic(), partitionDrained, event.isPaused());
    }
}
