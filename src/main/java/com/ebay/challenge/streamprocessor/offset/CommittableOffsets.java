package com.ebay.challenge.streamprocessor.offset;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;

/** Kafka offsets plus listener commit actions for the same contiguous prefixes. */
public record CommittableOffsets(
        Map<TopicPartition, OffsetAndMetadata> offsets,
        Map<TopicPartition, Runnable> commitActions
) {
    public CommittableOffsets {
        offsets = Collections.unmodifiableMap(offsets);
        commitActions = Collections.unmodifiableMap(commitActions);
    }

    public boolean isEmpty() {
        return offsets.isEmpty();
    }
}
