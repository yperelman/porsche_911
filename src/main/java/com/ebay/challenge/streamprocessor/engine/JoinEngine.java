package com.ebay.challenge.streamprocessor.engine;

import com.ebay.challenge.streamprocessor.backpressure.PartitionBackpressureService;
import com.ebay.challenge.streamprocessor.deadletter.DeadLetterPublisher;
import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.ebay.challenge.streamprocessor.model.StreamEvent;
import com.ebay.challenge.streamprocessor.observability.MetricsCounters;
import com.ebay.challenge.streamprocessor.observability.RecentEventLog;
import com.ebay.challenge.streamprocessor.offset.OffsetCommitTracker;
import com.ebay.challenge.streamprocessor.output.OutputSink;
import com.ebay.challenge.streamprocessor.output.OutputSinkFactory;
import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Update-style event-time join. Page views are written immediately using the
 * best click known at arrival time. Later clicks run an idempotent database-side
 * correction UPDATE for already-written page views where that click becomes the
 * latest eligible attribution. Page-view offsets commit after durable write;
 * click offsets stay replayable until watermark eviction so a restart can
 * rebuild click state and reapply corrections.
 */
@Component
public class JoinEngine {

    private final ClickStateStore clickStore;
    private final WatermarkTracker watermarks;
    private final OutputSinkFactory sinkFactory;
    private final DeadLetterPublisher deadLetterPublisher;
    private final OffsetCommitTracker offsetCommits;
    private final MetricsCounters metrics;
    private final RecentEventLog eventLog;
    private final PartitionBackpressureService partitionBackpressure;
    private final String clicksTopic;
    private final String pageViewsTopic;
    private final Clock clock;
    private final Duration maxFutureSkew;
    private final Duration attributionWindow;

    public JoinEngine(ClickStateStore clickStore, WatermarkTracker watermarks,
                      OutputSinkFactory sinkFactory,
                      DeadLetterPublisher deadLetterPublisher, OffsetCommitTracker offsetCommits,
                      MetricsCounters metrics, RecentEventLog eventLog,
                      PartitionBackpressureService partitionBackpressure,
                      @Value("${kafka.topics.ad-clicks:ad_clicks}") String clicksTopic,
                      @Value("${kafka.topics.page-views:page_views}") String pageViewsTopic,
                      Clock clock,
                      @Value("${watermark.max-future-skew-minutes:5}") long maxFutureSkewMinutes,
                      @Value("${attribution.window-minutes:30}") long attributionWindowMinutes) {
        this.clickStore = clickStore;
        this.watermarks = watermarks;
        this.sinkFactory = sinkFactory;
        this.deadLetterPublisher = deadLetterPublisher;
        this.offsetCommits = offsetCommits;
        this.metrics = metrics;
        this.eventLog = eventLog;
        this.partitionBackpressure = partitionBackpressure;
        this.clicksTopic = clicksTopic;
        this.pageViewsTopic = pageViewsTopic;
        this.clock = clock;
        this.maxFutureSkew = Duration.ofMinutes(maxFutureSkewMinutes);
        this.attributionWindow = Duration.ofMinutes(attributionWindowMinutes);
    }

    public void processClick(AdClickEvent click) {
        if (!handleInvalid(click, clicksTopic, click.getClickId())) {
            return;
        }
        boolean added = clickStore.addClick(click);
        if (!added) {
            eventLog.addStep(click.getClickId(), "duplicate/retried click (re-running correction)");
        } else {
            eventLog.addStep(click.getClickId(), "stored in click state");
            partitionBackpressure.onClickStored(click.getPartition(), clickStore.getClickCount(click.getPartition()));
        }
        watermarks.updateWatermark(click.getPartition(), clicksTopic, click.getEventTime());
        OutputSink sink = sinkFactory.sinkFor(click.getPartition());
        sink.updateClick(click.getUserId(), click.getEventTime(),
                click.getClickId(), click.getCampaignId(), attributionWindow);
        sink.flush();
        if (!added && clickStore.isDuplicateFromDifferentSourceOffset(click)) {
            // A distinct Kafka duplicate does not need to pin the commit prefix: the
            // retained source offset remains replayable until eviction.
            offsetCommits.markDone(new TopicPartition(clicksTopic, click.getPartition()), click.getOffset());
        }
        evictClicks(click.getPartition());
    }

    public void processPageView(PageViewEvent pageView) {
        if (!handleInvalid(pageView, pageViewsTopic, pageView.getEventId())) {
            return;
        }
        // Write immediately with the best click known now; later clicks correct the row in
        // place via correctForClick. The UPSERT is idempotent on page_view_id, so a replayed
        // duplicate is harmless (its conflict update is latest-click-wins guarded). The offset
        // is committed on write — correction state lives durably in Postgres, not in memory.
        AdClickEvent matched = clickStore.findAttributableClick(pageView.getUserId(), pageView.getEventTime());
        OutputSink sink = sinkFactory.sinkFor(pageView.getPartition());
        boolean inserted = sink.write(attributedOf(pageView, matched));
        sink.flush();
        offsetCommits.markDone(new TopicPartition(pageViewsTopic, pageView.getPartition()), pageView.getOffset());
        // Count metrics once per distinct page view, not once per (re)delivery: a duplicate
        // UPSERTs to the same row (inserted == false) and must not double-count.
        if (inserted) {
            recordVisibleAttribution(pageView, matched, "emitted");
            metrics.incrementPageViewsAttributed();
            if (matched != null) {
                metrics.recordCampaignCredit(matched.getCampaignId());
            }
            metrics.recordEndToEndLatency(Duration.between(pageView.getEventTime(), Instant.now()));
        }
        watermarks.updateWatermark(pageView.getPartition(), pageViewsTopic, pageView.getEventTime());
        evictClicks(pageView.getPartition());
    }

    private boolean handleInvalid(StreamEvent event, String topic, String id) {
        return handleInvalidData(event, topic, id)
                && handleTimeSkew(event, topic, id)
                && handleTooLate(event, topic, id);
    }

    /**
     * Rejects structurally-invalid events (missing {@code event_time} or {@code user_id})
     * to {@code dead_letter} before any watermark/store/sink work, instead of NPE-ing the
     * processing thread (which becomes a poison pill). Returns false if the event was rejected.
     */
    private boolean handleInvalidData(StreamEvent event, String topic, String id) {
        String reason = null;
        if (event.getEventTime() == null) {
            reason = "missing event_time";
        } else if (event.getUserId() == null || event.getUserId().isBlank()) {
            reason = "missing user_id";
        }
        if (reason != null) {
            return reject(event, topic, id, reason);
        }
        return true;
    }

    private boolean handleTimeSkew(StreamEvent event, String topic, String id) {
        Instant eventTime = event.getEventTime();
        boolean isTooFarInFuture = eventTime != null && eventTime.isAfter(clock.instant().plus(maxFutureSkew));
        if (isTooFarInFuture) {
            return reject(event, topic, id, "event_time too far in future");
        }
        return true;
    }

    /**
     * Rejects events past the allowed-lateness fence to {@code dead_letter}.
     * Returns false if the event was rejected.
     */
    private boolean handleTooLate(StreamEvent event, String topic, String id) {
        if (watermarks.isTooLate(event.getPartition(), event.getEventTime())) {
            return reject(event, topic, id, "event_time before watermark cutoff");
        }
        return true;
    }

    /**
     * Publishes a rejected event to {@code dead_letter} and marks its source offset done.
     * The DLQ publish blocks until durable, then the offset is marked done — the rejected
     * event has reached its terminal state and no longer blocks the partition's offset
     * commit progression. Always returns false (event was rejected).
     */
    private boolean reject(StreamEvent event, String topic, String id, String reason) {
        deadLetterPublisher.publish(topic, event.getPartition(), event.getOffset(), event.toString(), reason);
        offsetCommits.markDone(new TopicPartition(topic, event.getPartition()), event.getOffset());
        eventLog.addStep(id, "dropped: " + reason + " → dead-lettered");
        return false;
    }

    /**
     * Page-view offsets are now committed on write, so finalization only evicts
     * click state once the watermark proves no future page view can still need it.
     * Runs on every watermark advance.
     */
    private void evictClicks(int partition) {
        var joined = watermarks.getJoinedWatermark(partition);
        if (joined.isEmpty()) {
            return;
        }
        Duration lateness = watermarks.getAllowedLateness();

        Instant clickRetentionCutoff = joined.get().minus(lateness).minus(attributionWindow);
        List<AdClickEvent> evicted = clickStore.evictOldClicks(partition, clickRetentionCutoff);
        for (AdClickEvent click : evicted) {
            offsetCommits.markDone(new TopicPartition(clicksTopic, click.getPartition()), click.getOffset());
        }
        partitionBackpressure.onClicksEvicted(partition, clickStore.getClickCount(partition));
    }

    private void recordVisibleAttribution(PageViewEvent pv, AdClickEvent matched, String terminalLabel) {
        String attribution = matched == null
                ? "no attribution (null)"
                : "attributed → " + matched.getCampaignId() + " (click " + matched.getClickId() + ")";
        // Route through recordAttribution (not addStep): the live trace is usually
        // evicted by the time attribution lands, so this also indexes it durably for
        // the attribution timeline and keeps it inspectable.
        eventLog.recordAttribution(RecentEventLog.EventType.PAGE_VIEW, pv.getEventId(), pv.getUserId(),
                pv.getEventTime(), pv.getPartition(), pv.getOffset(), attribution, terminalLabel);
    }

    private AttributedPageView attributedOf(PageViewEvent pv, AdClickEvent matched) {
        return AttributedPageView.builder()
                .pageViewId(pv.getEventId())
                .userId(pv.getUserId())
                .eventTime(pv.getEventTime())
                .url(pv.getUrl())
                .attributedCampaignId(matched == null ? null : matched.getCampaignId())
                .attributedClickId(matched == null ? null : matched.getClickId())
                .attributedClickTime(matched == null ? null : matched.getEventTime())
                .build();
    }
}
