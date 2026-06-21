package com.ebay.challenge.streamprocessor.backpressure;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Exact backpressure: pause/resume only the Kafka topic-partition that is
 * creating pressure, and let Kafka hold backlog instead of emitting early or
 * evicting still-needed state.
 */
@Slf4j
@Component
public class PartitionBackpressureService {

    private final KafkaListenerEndpointRegistry listenerRegistry;
    private final String adClicksTopic;
    private final long clickStateHigh;
    private final long clickStateLow;
    private final Set<TopicPartition> pausedPartitions = ConcurrentHashMap.newKeySet();

    public PartitionBackpressureService(
            KafkaListenerEndpointRegistry listenerRegistry,
            @Value("${kafka.topics.ad-clicks:ad_clicks}") String adClicksTopic,
            @Value("${backpressure.partition.click-state.high:1000000}") long clickStateHigh,
            @Value("${backpressure.partition.click-state.low:500000}") long clickStateLow) {
        if (clickStateLow > clickStateHigh) {
            throw new IllegalArgumentException("click-state low watermark must be <= high watermark");
        }
        this.listenerRegistry = listenerRegistry;
        this.adClicksTopic = adClicksTopic;
        this.clickStateHigh = clickStateHigh;
        this.clickStateLow = clickStateLow;
    }

    public void onClickStored(int partition, long clickCount) {
        if (clickCount <= clickStateHigh) {
            return;
        }
        TopicPartition tp = new TopicPartition(adClicksTopic, partition);
        if (!pausedPartitions.add(tp)) {
            return;
        }
        log.warn("Pausing {} after partition state crossed high watermark: size={}, high={}",
                tp, clickCount, clickStateHigh);
        if (listenerRegistry != null) {
            listenerRegistry.getAllListenerContainers().forEach(container -> container.pausePartition(tp));
        }
    }

    public void onClicksEvicted(int partition, long clickCount) {
        if (clickCount > clickStateLow) {
            return;
        }
        TopicPartition tp = new TopicPartition(adClicksTopic, partition);
        if (!pausedPartitions.remove(tp)) {
            return;
        }
        log.info("Resuming {} after partition state dropped below low watermark: size={}, low={}",
                tp, clickCount, clickStateLow);
        if (listenerRegistry != null) {
            listenerRegistry.getAllListenerContainers().forEach(container -> container.resumePartition(tp));
        }
    }

}
