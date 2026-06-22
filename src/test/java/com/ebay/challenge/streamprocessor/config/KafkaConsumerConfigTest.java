package com.ebay.challenge.streamprocessor.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaConsumerConfigTest {

    @Test
    void newTopicBeansUseConfiguredConcurrencyAsPartitionCount() {
        KafkaConsumerConfig config = new KafkaConsumerConfig();
        ReflectionTestUtils.setField(config, "concurrency", 5);

        NewTopic adClicks = config.adClicksTopic("ad_clicks");
        NewTopic pageViews = config.pageViewsTopic("page_views");
        NewTopic deadLetter = config.deadLetterTopic("dead_letter");

        assertEquals(5, adClicks.numPartitions(), "ad_clicks partition count tracks concurrency");
        assertEquals(5, pageViews.numPartitions(), "page_views partition count tracks concurrency");
        assertEquals(5, deadLetter.numPartitions(), "dead_letter partition count tracks concurrency");
    }
}
