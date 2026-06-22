package com.ebay.challenge.streamprocessor.observability;

import com.ebay.challenge.streamprocessor.observability.MetricsCounters.CampaignCredit;
import com.ebay.challenge.streamprocessor.observability.MetricsCounters.LatencyBucket;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsCountersTest {

    @Test
    void topCampaignsRanksByCreditDescendingAndRespectsLimit() {
        MetricsCounters m = new MetricsCounters();
        for (int i = 0; i < 5; i++) m.recordCampaignCredit("camp_A");
        for (int i = 0; i < 3; i++) m.recordCampaignCredit("camp_B");
        m.recordCampaignCredit("camp_C");
        m.recordCampaignCredit(null); // ignored

        List<CampaignCredit> top2 = m.topCampaigns(2);
        assertEquals(List.of(new CampaignCredit("camp_A", 5), new CampaignCredit("camp_B", 3)), top2);
        assertEquals(3, m.topCampaigns(10).size(), "all three campaigns when limit exceeds count");
    }

    @Test
    void latencyHistogramBucketsByEndToEndLatency() {
        MetricsCounters m = new MetricsCounters();
        m.recordEndToEndLatency(Duration.ofMillis(400));  // ≤1s
        m.recordEndToEndLatency(Duration.ofSeconds(1));   // ≤1s
        m.recordEndToEndLatency(Duration.ofSeconds(3));   // 1–5s
        m.recordEndToEndLatency(Duration.ofSeconds(45));  // 15–60s
        m.recordEndToEndLatency(Duration.ofMinutes(10));  // >5m
        m.recordEndToEndLatency(Duration.ofSeconds(-7));  // clamped → ≤1s

        Map<String, Long> counts = m.latencyHistogram().stream()
                .collect(Collectors.toMap(LatencyBucket::label, LatencyBucket::count));
        assertEquals(3L, counts.get("≤1s"));
        assertEquals(1L, counts.get("1–5s"));
        assertEquals(0L, counts.get("5–15s"));
        assertEquals(1L, counts.get("15–60s"));
        assertEquals(0L, counts.get("1–5m"));
        assertEquals(1L, counts.get(">5m"));
    }

    @Test
    void histogramHasStableBucketOrderFastestFirst() {
        MetricsCounters m = new MetricsCounters();
        List<String> labels = m.latencyHistogram().stream().map(LatencyBucket::label).toList();
        assertEquals(List.of("≤1s", "1–5s", "5–15s", "15–60s", "1–5m", ">5m"), labels);
        assertTrue(m.latencyHistogram().stream().allMatch(b -> b.count() == 0), "starts empty");
    }
}
