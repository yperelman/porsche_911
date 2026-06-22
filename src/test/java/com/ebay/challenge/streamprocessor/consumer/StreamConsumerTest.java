package com.ebay.challenge.streamprocessor.consumer;

import com.ebay.challenge.streamprocessor.deadletter.DeadLetterPublisher;
import com.ebay.challenge.streamprocessor.engine.JoinEngine;
import com.ebay.challenge.streamprocessor.exception.PartitionTopologyException;
import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.ebay.challenge.streamprocessor.observability.MetricsCounters;
import com.ebay.challenge.streamprocessor.observability.RecentEventLog;
import com.ebay.challenge.streamprocessor.offset.OffsetCommitTracker;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class StreamConsumerTest {

    private static final DeadLetterPublisher DLQ = (topic, partition, offset, payload, reason) -> {};

    private static ObjectMapper mapper() {
        ObjectMapper m = new ObjectMapper();
        m.registerModule(new JavaTimeModule());
        return m;
    }

    @Test
    void consumeAdClick_parsesJson_registersOffset_routesToJoinEngine() {
        JoinEngine engine = mock(JoinEngine.class);
        OffsetCommitTracker offsets = mock(OffsetCommitTracker.class);
        StreamConsumer consumer = new StreamConsumer(engine, offsets, mapper(), new MetricsCounters(), new RecentEventLog(false, 0), DLQ, 3);

        String payload = """
                {"user_id":"user_1","event_time":"2024-01-01T12:00:00","campaign_id":"campaign_A","click_id":"click_1"}
                """;
        ConsumerRecord<String, String> record = new ConsumerRecord<>("ad_clicks", 2, 17L, "user_1", payload);

        consumer.consumeAdClick(record, null);

        verify(offsets).register(new TopicPartition("ad_clicks", 2), 17L);
        ArgumentCaptor<AdClickEvent> captor = ArgumentCaptor.forClass(AdClickEvent.class);
        verify(engine).processClick(captor.capture());
        AdClickEvent c = captor.getValue();
        assertEquals("user_1", c.getUserId());
        assertEquals("click_1", c.getClickId());
        assertEquals("campaign_A", c.getCampaignId());
        assertEquals(Instant.parse("2024-01-01T12:00:00Z"), c.getEventTime());
        assertEquals(2, c.getPartition());
        assertEquals(17L, c.getOffset());
    }

    @Test
    void consumePageView_parsesJson_registersOffset_routesToJoinEngine() {
        JoinEngine engine = mock(JoinEngine.class);
        OffsetCommitTracker offsets = mock(OffsetCommitTracker.class);
        StreamConsumer consumer = new StreamConsumer(engine, offsets, mapper(), new MetricsCounters(), new RecentEventLog(false, 0), DLQ, 3);

        String payload = """
                {"user_id":"user_2","event_time":"2024-01-01T12:10:00","url":"https://example.com/p","event_id":"pv_1"}
                """;
        ConsumerRecord<String, String> record = new ConsumerRecord<>("page_views", 1, 5L, "user_2", payload);

        consumer.consumePageView(record, null);

        verify(offsets).register(new TopicPartition("page_views", 1), 5L);
        ArgumentCaptor<PageViewEvent> captor = ArgumentCaptor.forClass(PageViewEvent.class);
        verify(engine).processPageView(captor.capture());
        PageViewEvent pv = captor.getValue();
        assertEquals("user_2", pv.getUserId());
        assertEquals("pv_1", pv.getEventId());
        assertEquals("https://example.com/p", pv.getUrl());
        assertEquals(Instant.parse("2024-01-01T12:10:00Z"), pv.getEventTime());
        assertEquals(1, pv.getPartition());
        assertEquals(5L, pv.getOffset());
    }

    @Test
    void malformedAdClick_doesNotThrow_andOffsetDoesNotStallPartition() {
        // Bug 1/2: a record that fails to parse currently throws (poison pill). Its offset
        // is registered before parsing and never marked done, so the partition's contiguous
        // commit prefix stalls permanently and the record is retried forever with no DLQ.
        // A malformed record must be handled gracefully: no throw, and its offset must become
        // commit-safe so later records on the partition can commit.
        JoinEngine engine = mock(JoinEngine.class);
        OffsetCommitTracker offsets = new OffsetCommitTracker(); // real tracker
        StreamConsumer consumer = new StreamConsumer(engine, offsets, mapper(), new MetricsCounters(), new RecentEventLog(false, 0), DLQ, 3);

        ConsumerRecord<String, String> bad = new ConsumerRecord<>("ad_clicks", 0, 0L, "u", "{not valid json");

        assertDoesNotThrow(() -> consumer.consumeAdClick(bad, null));
        verify(engine, never()).processClick(org.mockito.ArgumentMatchers.any());
        assertEquals(1L, offsets.drainCommittableBatch().offsets().get(new TopicPartition("ad_clicks", 0)).offset(),
                "malformed record's offset must be commit-safe, not a permanent gap");
    }

    @Test
    void recordFromPartitionOutsideTopology_failsFast() {
        JoinEngine engine = mock(JoinEngine.class);
        OffsetCommitTracker offsets = mock(OffsetCommitTracker.class);
        // Configured for 3 partitions (0,1,2); a record on partition 5 is outside the topology.
        StreamConsumer consumer = new StreamConsumer(engine, offsets, mapper(), new MetricsCounters(), new RecentEventLog(false, 0), DLQ, 3);

        String payload = """
                {"user_id":"user_1","event_time":"2024-01-01T12:00:00","campaign_id":"campaign_A","click_id":"click_1"}
                """;
        ConsumerRecord<String, String> record = new ConsumerRecord<>("ad_clicks", 5, 0L, "user_1", payload);

        assertThrows(PartitionTopologyException.class, () -> consumer.consumeAdClick(record, null));
        verify(engine, never()).processClick(org.mockito.ArgumentMatchers.any());
    }
}
