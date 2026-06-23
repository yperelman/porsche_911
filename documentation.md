# Implementation Documentation

## Summary

This processor is a Java 21 Spring Boot service using Spring Kafka and Postgres. It consumes `ad_clicks` and `page_views`, joins by `user_id` in event time, writes current attribution rows to Postgres, and publishes very-late events to Kafka topic `dead_letter`.

The implementation uses update-style attribution. A page view is written immediately using the best click currently known. A later eligible click runs an idempotent database `UPDATE` over already-written rows for the same user and attribution window. The sink exposes one current row per `page_view_id`.

## Join semantics

For each page view, the selected click is the latest click for the same user where:

```text
page_view_time - 30 minutes <= click_time <= page_view_time
```

If no click matches at page-view arrival, nullable attribution fields are written. A later accepted click can update that row if it is newer than the row's current `attributed_click_time`; ties use `click_id` ordering for deterministic behavior.

Clicks are stored in memory by `(user_id, event_time, click_id)`. The retained state entry's Kafka `(partition, offset)` remains replayable until watermark eviction. Duplicate or retried clicks still re-run the database correction. A retry from the retained source offset stays uncommitted; a duplicate logical click from a different source offset may become done after that correction is flushed because the retained offset still protects restart recovery.

## Watermark and late-data logic

Watermarks are tracked per `(topic, partition)`. For numeric partition `P`, the joined watermark is the monotonic minimum of `ad_clicks-P` and `page_views-P`, excluding only sources that are explicitly marked idle by Spring Kafka partition-idle events.

A source is idle only if the idle event proves it is inactive, caught up to the broker end offset, and not paused. Paused or backlogged partitions continue constraining the joined watermark. This prevents a partition paused for backpressure from having its queued records misclassified as late. A genuinely empty page-view partition can become idle, allowing click-state eviction to continue and preventing OOM.

An event is too late when:

```text
event_time <= joined_watermark - allowed_lateness
```

Too-late records are synchronously published to `dead_letter`; after broker acknowledgement, their source offsets become commit-safe.

Symmetrically, an event whose `event_time` is more than `max-future-skew-minutes` ahead of wall-clock (producer clock skew) is rejected to `dead_letter` **before** it touches the watermark. Otherwise a single future-dated event would advance the monotonic joined watermark permanently and strand the partition, dropping every later real-time event as too late.

## Output and delivery guarantees

Postgres table `attributed_page_views` is keyed by the composite primary key `(page_view_id, user_id)` — `page_view_id` is not guaranteed globally unique, so user_id is part of the identity. Page-view writes use UPSERT on that key. Click corrections use a guarded set-based `UPDATE` over rows where `click_time <= page_view_time <= click_time + 30 minutes` and only apply when the click wins latest-click ordering.

The guarantee is at-least-once processing with idempotent visible output:

| Failure | Outcome |
|---|---|
| Crash before Postgres flush | Transaction rolls back; source offsets stay uncommitted; records replay. |
| Crash after page-view flush | Row is durable and page-view offset may commit; recovery relies on Postgres durability. |
| Crash before click eviction | Click offset replays; correction UPDATE is reapplied idempotently. |
| Click correction flush fails, then record retries | Retry re-runs correction and flushes again before the retained click source offset can commit. |
| Duplicate logical click arrives at another Kafka offset | Correction is re-run and flushed; that duplicate offset may commit because the retained source offset still replays the click after restart. |
| Dead-letter publish succeeds but source offset does not commit | Late event may be published again after replay; deterministic source key enables dedupe/compaction. |
| Offset acknowledgement fails | Tracker retains the safe prefix and retries acknowledgement on the next maintenance cycle. |

The critical ordering is:

```text
durable Postgres/DLQ output -> mark offset done -> Kafka acknowledgement succeeds -> remove tracker entries
```

## Deduplication strategy

Both inputs are at-least-once, so duplicates are expected and handled per stream:

- **Page views** — deduped by the sink's idempotent UPSERT on the composite primary key
  `(page_view_id, user_id)`. A duplicate (Kafka redelivery or a re-sent id) maps to the same
  row, so there is exactly one current row per `(page_view_id, user_id)`. The UPSERT conflict
  clause is latest-click-wins guarded, so re-writing a duplicate never regresses a correction
  already applied by a later click. `user_id` is part of the key because `page_view_id` is not
  globally unique — two users may share an id and must remain separate rows.
- **Clicks** — deduped in memory by `ClickStateStore`, keyed `(event_time, click_id)` per user.
  `isDuplicateFromDifferentSourceOffset` separates a retry of the retained Kafka source offset
  (stays replayable until watermark eviction) from a duplicate logical click delivered at a
  different offset (may commit once its idempotent correction UPDATE is flushed). `click_id`
  gives clicks a stable identity for dedup and a deterministic latest-click tie-break.

Caveat: a duplicate page view re-runs the per-event metrics (attributed counter, campaign
credit, end-to-end latency), so those counters can over-count on redelivery even though the
visible row stays single. This is an observability inaccuracy only; the attributed output is
correct.

## Offset strategy

`OffsetCommitTracker` records every consumed source offset and marks it done only after its work is durable. `OffsetCommitScheduler` periodically prepares the highest contiguous done prefix for each topic-partition. Preparation is non-destructive; entries are removed only after the acknowledgement action succeeds.

Page-view offsets are marked done after their UPSERT is flushed. Retained click source offsets are marked done when click state is evicted after the attribution window plus lateness. This keeps clicks replayable long enough to rebuild state and reapply corrections after a restart.

Duplicate click handling intentionally distinguishes source offsets. `ClickStateStore.isDuplicateFromDifferentSourceOffset(...)` returns true only when the incoming record has the same logical click key as a retained click but a different Kafka `(partition, offset)`. That predicate allows the duplicate offset to commit without committing the retained source offset early.

## Concurrency and state

The Kafka topics must be co-partitioned by `user_id`. Spring Kafka runs concurrent listeners, and `StreamConsumer` serializes both topics for the same numeric partition through a partition lock. Different partitions can process in parallel.

Each numeric partition has its own Postgres sink connection. Click state is partition-scoped for eviction and backpressure. State size is approximately:

```text
click_rate * (30 minutes + allowed_lateness)
```

When click state for a partition crosses the high watermark, only that `ad_clicks-P` partition is paused. It resumes after state drops below the low watermark. Postgres failures use a separate global pause and health probe.

## Tests

The suite covers latest-click attribution, out-of-order correction, late-event dead letters, retry after click correction flush failure, duplicate handling, non-destructive offset acknowledgement retry, Kafka-aware watermark idleness, restart behavior, concurrent partition sink isolation, backpressure behavior, and dashboard metrics.

Run:

```bash
mvn test
```

Testcontainers tests require Docker socket access.
