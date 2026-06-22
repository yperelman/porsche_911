package com.ebay.challenge.streamprocessor.deadletter;

import com.ebay.challenge.streamprocessor.observability.MetricsCounters;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka-backed dead-letter publisher. Publishes a JSON envelope to a dedicated
 * dead-letter topic and blocks until the broker acks the send. Synchronous
 * by design: the caller relies on this method's return as the durability
 * barrier, then marks the source offset commit-safe.
 */
@Component
public class KafkaDeadLetterPublisher implements DeadLetterPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String deadLetterTopic;
    private final long sendTimeoutMs;
    private final MetricsCounters metrics;
    private final ObjectMapper objectMapper;

    public KafkaDeadLetterPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${kafka.topics.dead-letter:dead_letter}") String deadLetterTopic,
            @Value("${kafka.dead-letter.send-timeout-ms:5000}") long sendTimeoutMs,
            MetricsCounters metrics,
            ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.deadLetterTopic = deadLetterTopic;
        this.sendTimeoutMs = sendTimeoutMs;
        this.metrics = metrics;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(String sourceTopic, int partition, long offset, String payload, String reason) {
        String envelope = envelope(sourceTopic, partition, offset, payload, reason);
        // Key by sourceTopic so each source-partition's dead-letters stay co-located,
        // which makes downstream inspection deterministic.
        String key = sourceTopic + ":" + partition + ":" + offset;
        try {
            SendResult<String, String> result = kafkaTemplate
                    .send(deadLetterTopic, key, envelope)
                    .get(sendTimeoutMs, TimeUnit.MILLISECONDS);
            // Successful return = broker ack; the source offset is now commit-safe.
            if (result.getRecordMetadata() == null) {
                throw new IllegalStateException("dead-letter send returned without record metadata");
            }
            metrics.incrementDeadLetterPublished();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("dead-letter publish interrupted", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException("dead-letter publish failed for " + key, e);
        }
    }

    private String envelope(String sourceTopic, int partition, long offset, String payload, String reason) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("source_topic", sourceTopic);
        envelope.put("partition", partition);
        envelope.put("offset", offset);
        envelope.put("reason", reason);
        envelope.put("payload", payload);
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("dead-letter envelope serialization failed", e);
        }
    }
}
