package com.ebay.challenge.streamprocessor.engine;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.ebay.challenge.streamprocessor.output.OutputSink;
import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Core join engine that performs windowed attribution joins between page views and ad clicks.
 *
 * Join semantics:
 * - For each page_view, find the most recent ad_click for the same user
 *   within 30 minutes before the page view (in event time)
 * - Handle out-of-order arrivals through watermark tracking
 *
 * TODO: Implement the windowed join logic
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JoinEngine {

    private final ClickStateStore clickStore;
    private final WatermarkTracker watermarkTracker;
    private final OutputSink outputSink;

    /**
     * Process an ad click event.
     * Store the click in state for future attribution.
     *
     * TODO: Implement click processing logic
     * - Check if event is too late using watermarkTracker
     * - Store the click in clickStore
     * - Update watermark for the partition
     *
     * @param click the ad click event
     */
    public void processClick(AdClickEvent click) {
        // TODO: Implement this method
        log.debug("Processing click: {}", click.getClickId());
    }

    /**
     * Process a page view event.
     * Find matching click and emit attributed page view.
     *
     * TODO: Implement page view processing logic
     * - Check if event is too late using watermarkTracker
     * - Find attributable click from clickStore
     * - Create and emit AttributedPageView
     * - Update watermark for the partition
     *
     * @param pageView the page view event
     */
    public void processPageView(PageViewEvent pageView) {
        // TODO: Implement this method
        log.debug("Processing page view: {}", pageView.getEventId());
    }

    /**
     * Scheduled task to evict old clicks from state.
     * Runs every 30 seconds to prevent unbounded memory growth.
     *
     * TODO: Implement state eviction logic
     * - Evict clicks older than the watermark cutoff
     * - Use clickStore.evictOldClicks() with appropriate cutoff time
     */
    @Scheduled(fixedRate = 30000)
    public void evictOldClicks() {
        // TODO: Implement this method
        log.debug("Running state eviction");
    }
}
