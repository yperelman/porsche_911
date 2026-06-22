package com.ebay.challenge.streamprocessor.deadletter;

import com.ebay.challenge.streamprocessor.observability.MetricsCounters;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaDeadLetterPublisherTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void publishSendsJacksonSerializedEnvelopeWithDeterministicKey() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(eq("dead_letter"), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(sendResult()));
        MetricsCounters metrics = new MetricsCounters();
        KafkaDeadLetterPublisher publisher = new KafkaDeadLetterPublisher(
                kafkaTemplate, "dead_letter", 5000, metrics, objectMapper);

        String payload = "{\"id\":\"pv_1\",\"nested\":{\"x\":1}}\n";
        String reason = "late \"event\"\tcutoff";
        publisher.publish("page_views", 2, 123L, payload, reason);

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("dead_letter"), key.capture(), value.capture());

        assertEquals("page_views:2:123", key.getValue());
        JsonNode envelope = objectMapper.readTree(value.getValue());
        assertEquals("page_views", envelope.get("source_topic").asText());
        assertEquals(2, envelope.get("partition").asInt());
        assertEquals(123L, envelope.get("offset").asLong());
        assertEquals(reason, envelope.get("reason").asText());
        assertTrue(envelope.get("payload").isTextual());
        assertEquals(payload, envelope.get("payload").asText());
        assertEquals(1, metrics.getDeadLetterPublished());
    }

    private static SendResult<String, String> sendResult() {
        ProducerRecord<String, String> record = new ProducerRecord<>("dead_letter", "key", "value");
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("dead_letter", 0), 0, 0, 0L, 0, 0);
        return new SendResult<>(record, metadata);
    }
}
