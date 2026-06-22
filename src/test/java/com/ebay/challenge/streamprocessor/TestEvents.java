package com.ebay.challenge.streamprocessor;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;

import java.time.Duration;
import java.time.Instant;

/** Dumb event builders for tests. No assertions or hidden behavior. */
public final class TestEvents {

    public static final Instant BASE = Instant.parse("2024-01-01T12:00:00Z");

    private TestEvents() {}

    public static Instant atMinute(long minute) {
        return BASE.plus(Duration.ofMinutes(minute));
    }

    public static AdClickEvent click(String userId, long eventTimeMinute,
                                     String clickId, String campaignId,
                                     int partition, long offset) {
        return click(userId, atMinute(eventTimeMinute), clickId, campaignId, partition, offset);
    }

    public static AdClickEvent click(String userId, Instant eventTime,
                                     String clickId, String campaignId,
                                     int partition, long offset) {
        return AdClickEvent.builder()
                .userId(userId)
                .eventTime(eventTime)
                .clickId(clickId)
                .campaignId(campaignId)
                .partition(partition)
                .offset(offset)
                .build();
    }

    public static PageViewEvent pv(String userId, long eventTimeMinute,
                                   String eventId, int partition, long offset) {
        return pv(userId, atMinute(eventTimeMinute), eventId, partition, offset);
    }

    public static PageViewEvent pv(String userId, Instant eventTime,
                                   String eventId, int partition, long offset) {
        return PageViewEvent.builder()
                .userId(userId)
                .eventTime(eventTime)
                .eventId(eventId)
                .url("https://example.com/" + eventId)
                .partition(partition)
                .offset(offset)
                .build();
    }

    public static AttributedPageView attributed(String pageViewId, String campaignId) {
        return AttributedPageView.builder()
                .pageViewId(pageViewId)
                .userId("u")
                .eventTime(BASE)
                .url("/" + pageViewId)
                .attributedCampaignId(campaignId)
                .attributedClickId(campaignId + "_clk")
                .build();
    }
}
