package com.ebay.challenge.streamprocessor.observability;

import com.ebay.challenge.streamprocessor.observability.RecentEventLog.EventTrace;
import com.ebay.challenge.streamprocessor.output.BackpressureCoordinator;
import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Live observability endpoint backing {@code /dashboard.html}.
 *
 * <p>{@code /api/dashboard} returns a single JSON snapshot combining current
 * state (click count, joined watermark + processing lag per partition, sink
 * paused flag) with cumulative process-lifetime
 * counters. {@code /api/events/*} expose the recent-event journeys backing the
 * live stream, attribution timeline, and event inspector. Values are pulled
 * directly from the live components, so they are consistent with what the engine
 * is actually doing at the moment of the call.
 */
@RestController
@RequestMapping("/api")
public class DashboardController {

    private final ClickStateStore clickStore;
    private final WatermarkTracker watermarks;
    private final BackpressureCoordinator backpressure;
    private final MetricsCounters counters;
    private final RecentEventLog eventLog;
    private final Clock clock;
    private final int partitionCount;

    public DashboardController(
            ClickStateStore clickStore,
            WatermarkTracker watermarks,
            BackpressureCoordinator backpressure,
            MetricsCounters counters,
            RecentEventLog eventLog,
            Clock clock,
            @Value("${kafka.consumer.concurrency:3}") int partitionCount) {
        this.clickStore = clickStore;
        this.watermarks = watermarks;
        this.backpressure = backpressure;
        this.counters = counters;
        this.eventLog = eventLog;
        this.clock = clock;
        this.partitionCount = partitionCount;
    }

    @GetMapping("/dashboard")
    public DashboardSnapshot snapshot() {
        Map<Integer, Instant> joinedByPartition = new LinkedHashMap<>();
        Map<Integer, Long> lagByPartition = new LinkedHashMap<>();
        Instant now = clock.instant();
        for (int p = 0; p < partitionCount; p++) {
            var joined = watermarks.getJoinedWatermark(p);
            joinedByPartition.put(p, joined.orElse(null));
            // Processing staleness = wall-clock since this partition's watermark last
            // advanced. Unlike (wall-clock − event-time watermark), this stays meaningful
            // for historical/backfill data (whose event_times are far from "now"): it
            // measures whether the pipeline is making progress, not how old the data is.
            // Null until the partition has advanced at least once → UI shows "—".
            lagByPartition.put(p, watermarks.lastProgressAt(p)
                    .map(advancedAt -> Duration.between(advancedAt, now).getSeconds())
                    .orElse(null));
        }
        return new DashboardSnapshot(
                clickStore.getTotalClickCount(),
                backpressure.isPaused(),
                joinedByPartition,
                lagByPartition,
                Map.of(
                        "clicksConsumed",        counters.getClicksConsumed(),
                        "pageViewsConsumed",     counters.getPageViewsConsumed(),
                        "pageViewsAttributed",   counters.getPageViewsAttributed(),
                        "deadLetterPublished",   counters.getDeadLetterPublished()
                )
        );
    }

    /** Live event stream feed: newest-first compact summaries of recent events. */
    @GetMapping("/events/recent")
    public List<EventSummary> recentEvents(@RequestParam(defaultValue = "50") int limit) {
        return eventLog.recent(limit).stream().map(DashboardController::summarize).toList();
    }

    /** Attribution timeline: recent events that reached an attribution. */
    @GetMapping("/events/attributions")
    public List<EventSummary> recentAttributions(@RequestParam(defaultValue = "50") int limit) {
        return eventLog.recentAttributions(limit).stream().map(DashboardController::summarize).toList();
    }

    /** Leaderboard: campaigns by credited page views, highest first. */
    @GetMapping("/campaigns/top")
    public List<MetricsCounters.CampaignCredit> topCampaigns(@RequestParam(defaultValue = "10") int limit) {
        return counters.topCampaigns(limit);
    }

    /** End-to-end latency distribution: page view event_time → attributed row write. */
    @GetMapping("/latency/histogram")
    public List<MetricsCounters.LatencyBucket> latencyHistogram() {
        return counters.latencyHistogram();
    }

    /** Event inspector: the full step-by-step journey of a single event. */
    @GetMapping("/events/{id}")
    public ResponseEntity<EventTrace> eventById(@PathVariable String id) {
        EventTrace trace = eventLog.byId(id);
        return trace == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(trace);
    }

    private static EventSummary summarize(EventTrace t) {
        return new EventSummary(
                t.getType().name(),
                t.getId(),
                t.getUserId(),
                t.getEventTime(),
                t.getPartition(),
                RecentEventLog.latestLabelOf(t));
    }

    public record DashboardSnapshot(
            long totalClicks,
            boolean sinkPaused,
            Map<Integer, Instant> joinedWatermarkByPartition,
            Map<Integer, Long> lagSecondsByPartition,
            Map<String, Long> counters
    ) {}

    public record EventSummary(
            String type,
            String id,
            String userId,
            Instant eventTime,
            int partition,
            String latestStep
    ) {}
}
