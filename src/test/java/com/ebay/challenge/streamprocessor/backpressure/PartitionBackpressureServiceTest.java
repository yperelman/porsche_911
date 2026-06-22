package com.ebay.challenge.streamprocessor.backpressure;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PartitionBackpressureServiceTest {

    @Test
    void pausesOnlyClickStatePartitionAboveHighWatermark_andResumesBelowLowWatermark() {
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        when(registry.getAllListenerContainers()).thenReturn(List.of(container));
        PartitionBackpressureService service = new PartitionBackpressureService(
                registry, "ad_clicks", 100, 50);

        service.onClickStored(2, 101);

        TopicPartition adClicks2 = new TopicPartition("ad_clicks", 2);
        verify(container).pausePartition(adClicks2);

        service.onClicksEvicted(2, 60);
        verify(container, never()).resumePartition(adClicks2);

        service.onClicksEvicted(2, 50);
        verify(container).resumePartition(adClicks2);
    }

    @Test
    void pauseIsIdempotentUntilResumeThresholdIsReached() {
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        when(registry.getAllListenerContainers()).thenReturn(List.of(container));
        PartitionBackpressureService service = new PartitionBackpressureService(
                registry, "ad_clicks", 3, 1);

        service.onClickStored(0, 4);
        service.onClickStored(0, 5);

        TopicPartition adClicks0 = new TopicPartition("ad_clicks", 0);
        verify(container, times(1)).pausePartition(adClicks0);

        service.onClicksEvicted(0, 2);
        verify(container, never()).resumePartition(adClicks0);

        service.onClicksEvicted(0, 1);
        verify(container, times(1)).resumePartition(adClicks0);
    }
}
