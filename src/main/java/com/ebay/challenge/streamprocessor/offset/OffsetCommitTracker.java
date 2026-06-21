package com.ebay.challenge.streamprocessor.offset;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tracks offset durability per topic-partition. A consumed record is
 * {@link #register(TopicPartition, long)}-ed when received and
 * {@link #markDone(TopicPartition, long)}-ed when its work is durable.
 * <p>
 * {@link #drainCommittableBatch()} returns the *contiguous done prefix* for
 * each partition — Kafka requires offsets to be committed as a prefix, so an
 * offset whose predecessors aren't done yet cannot be committed even if it's
 * itself done.
 */
@Slf4j
@Component
public class OffsetCommitTracker {

    // Outer key: TopicPartition. Inner: offset -> done flag + commit action.
    private final Map<TopicPartition, ConcurrentSkipListMap<Long, TrackedOffset>> pending = new ConcurrentHashMap<>();

    public void register(TopicPartition tp, long offset) {
        register(tp, offset, null);
    }

    public void register(TopicPartition tp, long offset, Runnable commitAction) {
        pending.computeIfAbsent(tp, k -> new ConcurrentSkipListMap<>())
                .putIfAbsent(offset, new TrackedOffset(commitAction));
    }

    public void markDone(TopicPartition tp, long offset) {
        ConcurrentSkipListMap<Long, TrackedOffset> partitionMap = pending.get(tp);
        if (partitionMap == null) {
            return;
        }
        TrackedOffset tracked = partitionMap.get(offset);
        if (tracked != null) {
            tracked.done.set(true);
        }
    }

    /**
     * Returns the contiguous done prefix per partition, plus the commit action
     * for the highest committed record in each partition.
     */
    public CommittableOffsets drainCommittableBatch() {
        Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
        Map<TopicPartition, Runnable> commitActions = new HashMap<>();
        for (Map.Entry<TopicPartition, ConcurrentSkipListMap<Long, TrackedOffset>> entry : pending.entrySet()) {
            ConcurrentSkipListMap<Long, TrackedOffset> partitionMap = entry.getValue();
            Long highestDone = null;
            Runnable highestDoneAction = null;
            for (Map.Entry<Long, TrackedOffset> tracked : partitionMap.entrySet()) {
                if (!tracked.getValue().done.get()) {
                    break;
                }
                highestDone = tracked.getKey();
                highestDoneAction = tracked.getValue().commitAction;
            }
            if (highestDone != null) {
                result.put(entry.getKey(), new OffsetAndMetadata(highestDone + 1));
                if (highestDoneAction != null) {
                    commitActions.put(entry.getKey(), highestDoneAction);
                }
            }
        }
        return new CommittableOffsets(result, commitActions);
    }

    /**
     * Commits every committable prefix to Kafka (runs each partition's commit action),
     * then removes only the prefixes whose acknowledgement succeeded. Called on the
     * maintenance cadence to advance the consumer-group offset frontier.
     */
    public void commitReadyOffsets() {
        CommittableOffsets committable = drainCommittableBatch();
        if (committable.isEmpty()) {
            return;
        }
        commit(committable);
        confirmCommitted(committable);
        log.info("committed offsets: {}", committable.offsets());
    }

    private void commit(CommittableOffsets offsets) {
        for (var entry : offsets.offsets().entrySet()) {
            offsets.commitActions().get(entry.getKey()).run();
        }
    }

    /** Remove only prefixes whose Kafka acknowledgement completed successfully. */
    public void confirmCommitted(CommittableOffsets committed) {
        for (var entry : committed.offsets().entrySet()) {
            ConcurrentSkipListMap<Long, TrackedOffset> partitionMap = pending.get(entry.getKey());
            if (partitionMap == null) {
                continue;
            }
            long nextOffset = entry.getValue().offset();
            partitionMap.headMap(nextOffset, false).clear();
            if (partitionMap.isEmpty()) {
                pending.remove(entry.getKey(), partitionMap);
            }
        }
    }

    private static class TrackedOffset {
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final Runnable commitAction;

        private TrackedOffset(Runnable commitAction) {
            this.commitAction = commitAction;
        }
    }
}
