package com.ebay.challenge.streamprocessor.observability;

import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Cumulative process-lifetime counters surfaced to the live dashboard.
 *
 * <p>Updated by hot-path components (consumer, join engine, dead-letter
 * publisher) and read by {@code DashboardController}. Lock-free reads via
 * {@link AtomicLong}/{@link LongAdder} so the dashboard polling thread never
 * contends with the listener threads.
 */
@Component
public class MetricsCounters {

    private final AtomicLong clicksConsumed = new AtomicLong();
    private final AtomicLong pageViewsConsumed = new AtomicLong();
    private final AtomicLong pageViewsAttributed = new AtomicLong();
    private final AtomicLong deadLetterPublished = new AtomicLong();

    public void incrementClicksConsumed()       { clicksConsumed.incrementAndGet(); }
    public void incrementPageViewsConsumed()    { pageViewsConsumed.incrementAndGet(); }
    public void incrementPageViewsAttributed()  { pageViewsAttributed.incrementAndGet(); }
    public void incrementDeadLetterPublished()  { deadLetterPublished.incrementAndGet(); }

    public long getClicksConsumed()        { return clicksConsumed.get(); }
    public long getPageViewsConsumed()     { return pageViewsConsumed.get(); }
    public long getPageViewsAttributed()   { return pageViewsAttributed.get(); }
    public long getDeadLetterPublished()   { return deadLetterPublished.get(); }

    // ---- Campaign credit -------------------------------------------------
    // Page views credited per campaign, for the dashboard leaderboard. A new
    // campaign id is rare relative to traffic, so a per-key LongAdder keeps the
    // hot path contention-free.
    private final Map<String, LongAdder> campaignCredits = new ConcurrentHashMap<>();

    public void recordCampaignCredit(String campaignId) {
        if (campaignId == null) {
            return;
        }
        campaignCredits.computeIfAbsent(campaignId, k -> new LongAdder()).increment();
    }

    // Note: credit is recorded only at a page view's initial write. Corrections are
    // applied by a set-based Postgres UPDATE with no per-row result, so the leaderboard
    // reflects initial attribution, not later corrections.

    public record CampaignCredit(String campaignId, long count) {}

    /** Campaigns by credited page views, highest first, capped at {@code limit}. */
    public List<CampaignCredit> topCampaigns(int limit) {
        return campaignCredits.entrySet().stream()
                .map(e -> new CampaignCredit(e.getKey(), e.getValue().sum()))
                .sorted(Comparator.comparingLong(CampaignCredit::count).reversed())
                .limit(Math.max(0, limit))
                .toList();
    }

    // ---- End-to-end latency histogram ------------------------------------
    // Wall-clock time between a page view's event_time and when its attributed row
    // was written. Fixed buckets keep the structure tiny and lock-free.
    private static final long[] BUCKET_UPPER_SECONDS = {1, 5, 15, 60, 300}; // last bucket = overflow
    private static final String[] BUCKET_LABELS = {"≤1s", "1–5s", "5–15s", "15–60s", "1–5m", ">5m"};
    private final AtomicLong[] latencyBuckets = newLatencyBuckets();

    private static AtomicLong[] newLatencyBuckets() {
        AtomicLong[] buckets = new AtomicLong[BUCKET_LABELS.length];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new AtomicLong();
        }
        return buckets;
    }

    public void recordEndToEndLatency(Duration latency) {
        long seconds = Math.max(0, latency.getSeconds());
        int bucket = BUCKET_UPPER_SECONDS.length; // default to overflow bucket
        for (int i = 0; i < BUCKET_UPPER_SECONDS.length; i++) {
            if (seconds <= BUCKET_UPPER_SECONDS[i]) {
                bucket = i;
                break;
            }
        }
        latencyBuckets[bucket].incrementAndGet();
    }

    public record LatencyBucket(String label, long count) {}

    /** Latency distribution in fixed buckets, fastest first. */
    public List<LatencyBucket> latencyHistogram() {
        List<LatencyBucket> out = new ArrayList<>(BUCKET_LABELS.length);
        for (int i = 0; i < BUCKET_LABELS.length; i++) {
            out.add(new LatencyBucket(BUCKET_LABELS[i], latencyBuckets[i].get()));
        }
        return out;
    }
}
