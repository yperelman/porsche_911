# Design — Real-time Session Attribution Stream Processor

## Product note

If an ad click directly opens a page, carrying `campaign_id` and `click_id` through the URL, cookie, or request context would be simpler and more accurate than inferring attribution from time. This implementation assumes those signals are unavailable and implements the requested event-time latest-click join.

## Architecture

```text
Kafka ad_clicks/page_views
        |
        v
StreamConsumer
  - parses records
  - registers offsets
  - serializes work by numeric partition
        |
        v
JoinEngine
  - ClickStateStore
  - WatermarkTracker
  - PostgresOutputSink per partition
  - Kafka dead-letter publisher
        |
        v
Postgres attributed_page_views / Kafka dead_letter
```

Two Kafka topics, `ad_clicks` and `page_views`, are treated as co-partitioned join shards. Numeric partition `N` across both topics owns one lock, one click-state shard, one joined watermark, and one Postgres sink connection.

## Join and output model

The output model is update-style, not emit-only-final:

1. A page view is written immediately with the best click currently in `ClickStateStore`.
2. A later accepted click executes a database-side correction UPDATE for the same user and `[click_time, click_time + 30 minutes]` page-view interval.
3. The Postgres row is guarded by latest-click-wins semantics, with `click_id` as deterministic tie-breaker.

This avoids in-memory page-view buffering. Correction state lives durably in Postgres via `attributed_click_time`, while click state remains in memory until watermark eviction.

## Watermark and idleness design

The joined watermark is the monotonic minimum of the two topic watermarks for a numeric partition. A source may be excluded from that minimum only when Spring Kafka reports a partition-idle event and the live consumer is caught up to the broker end offset and unpaused.

This distinction matters:

- idle and caught-up page-view partitions can be excluded, preventing click-state OOM;
- paused or backlogged partitions still constrain the watermark, preventing queued events from being incorrectly dead-lettered;
- when a source resumes, the joined watermark does not regress.

Late events are dead-lettered when `event_time <= joined_watermark - allowed_lateness`.

## Offset and recovery design

Offsets are tracked independently from business rows. The processor follows:

```text
durable output -> markDone -> Kafka ack succeeds -> remove tracker entries
```

The tracker prepares committable contiguous prefixes non-destructively. If acknowledgement fails, the same prefix is retried later. Page-view offsets become done after the flushed UPSERT. Retained click source offsets become done after click eviction; this keeps clicks replayable through their correction window. Duplicate logical clicks from different Kafka source offsets can commit after their correction flush because the retained source offset still protects replay.

## Capacity and backpressure

Click state is bounded by approximately `click_rate * (30 minutes + allowed_lateness)`. If a partition crosses the high watermark, only that `ad_clicks` topic-partition pauses; Kafka holds the backlog. Because paused partitions are not idle, this does not corrupt watermark correctness. Global sink failure pauses all listeners and a health probe resumes them after Postgres recovers.

## Testing and operation

`README.md` contains local setup, run, and verification commands. `documentation.md` contains the detailed implementation contract and failure-mode table. The test suite includes deterministic join cases, retry/offset/watermark regressions, restart behavior, and concurrent partition checks.
