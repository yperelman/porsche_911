package com.ebay.challenge.streamprocessor.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for concurrent, partition-aware processing.
 *
 * Key design decisions:
 * - Manual acknowledgment mode for offset control
 * - Concurrent consumers (one thread per partition)
 * - Disable auto-commit for safety
 * - Enable idempotence through consumer configuration
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${kafka.bootstrap-servers:localhost:29092}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group-id:stream-processor-group}")
    private String groupId;

    @Value("${kafka.consumer.concurrency:3}")
    private int concurrency;

    @Value("${watermark.idle-timeout-seconds:60}")
    private long idleTimeoutSeconds;

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        // Serialize Instants as ISO-8601 strings (rather than epoch numbers) — used
        // both by the Kafka consumer (when parsing input) and by the dashboard JSON
        // controller (so timestamps render nicely in the browser).
        mapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

    /**
     * KafkaAdmin auto-applies the {@link NewTopic} beans below at startup,
     * creating topics with partition counts equal to {@code kafka.consumer.concurrency}
     * so the per-partition concurrency story is demonstrable end-to-end (not silently
     * undercut by a default 1-partition topic from broker auto-create). Binding both to
     * the same property keeps {@link TopicTopologyValidator}'s actual-vs-expected check
     * consistent by construction.
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(config);
    }

    @Bean
    public NewTopic adClicksTopic(@Value("${kafka.topics.ad-clicks:ad_clicks}") String name) {
        return new NewTopic(name, concurrency, (short) 1);
    }

    @Bean
    public NewTopic pageViewsTopic(@Value("${kafka.topics.page-views:page_views}") String name) {
        return new NewTopic(name, concurrency, (short) 1);
    }

    /** Dead-letter topic for events that arrived past the allowed-lateness fence. */
    @Bean
    public NewTopic deadLetterTopic(@Value("${kafka.topics.dead-letter:dead_letter}") String name) {
        return new NewTopic(name, concurrency, (short) 1);
    }

    /**
     * Producer factory for the dead-letter publisher. acks=all + idempotence so
     * that send().get() returning ack is a true durability barrier — and that
     * intermittent broker hiccups don't produce duplicate dead-letter envelopes.
     */
    @Bean
    public ProducerFactory<String, String> deadLetterProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> deadLetterKafkaTemplate(ProducerFactory<String, String> deadLetterProducerFactory) {
        return new KafkaTemplate<>(deadLetterProducerFactory);
    }

    /**
     * Base consumer configuration shared by all consumers.
     */
    private Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();

        // Connection
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Deserialization
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Offset management - MANUAL control for at-least-once delivery
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Performance and reliability
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5 minutes
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000); // 30 seconds
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000); // 10 seconds

        // Fetch configuration
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        // Isolation level for exactly-once semantics (if producers use transactions)
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return props;
    }

    /**
     * Consumer factory for ad click events.
     */
    @Bean
    public ConsumerFactory<String, String> adClickConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    /**
     * Listener container factory for ad click events.
     * Configured for concurrent processing with manual acknowledgment.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> adClickListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(adClickConsumerFactory());

        // Concurrency: one thread per partition (up to configured max)
        factory.setConcurrency(concurrency);

        // Manual acknowledgment for offset control
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // Preserve partition ordering within each partition
        factory.getContainerProperties().setMissingTopicsFatal(false);
        factory.getContainerProperties().setIdlePartitionEventInterval(idleTimeoutSeconds * 1000L);

        return factory;
    }

    /**
     * Consumer factory for page view events.
     */
    @Bean
    public ConsumerFactory<String, String> pageViewConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    /**
     * Listener container factory for page view events.
     * Configured for concurrent processing with manual acknowledgment.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> pageViewListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(pageViewConsumerFactory());

        // Concurrency: one thread per partition (up to configured max)
        factory.setConcurrency(concurrency);

        // Manual acknowledgment for offset control
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // Preserve partition ordering within each partition
        factory.getContainerProperties().setMissingTopicsFatal(false);
        factory.getContainerProperties().setIdlePartitionEventInterval(idleTimeoutSeconds * 1000L);

        return factory;
    }
}
