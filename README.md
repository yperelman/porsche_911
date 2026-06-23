# Real-time Session Attribution with Windowed Stream Joins

## Implementation Summary

This repository contains a Java 21 / Spring Boot stream processor that consumes `ad_clicks` and `page_views` Kafka topics, joins records by `user_id` in event time, and writes the current attribution result to Postgres table `attributed_page_views`.

The processor uses update-style output: a page view is written immediately with the best click known at arrival time, and later eligible clicks correct the same row with an idempotent Postgres `UPSERT`. The visible sink has one current row per `page_view_id`.

Given the stubs that were given with the project I understood that solutions like Flink for stream processing or Redis for shared state management should not be used here, even though a lot of the complexity of this project could have been resolved by them.

### Prerequisites

- Java 21
- Maven
- Docker / Docker Compose
- Python 3.11+ for the sample data generator

## Product Tradeoff

In a real product, I would first push back on this design. If an ad click opens a page, carrying `campaign_id` and `click_id` through the URL, cookie, or request context is cheaper and more accurate than inferring causality from a time window.

The implemented join assumes that application-level attribution is unavailable. It follows last-touch semantics: each page view independently picks the latest eligible prior click, and a click may be reused for multiple page views. This is deterministic, but it cannot prove true causality when a user clicks multiple ads quickly.


### Architecture

- Input topics: `ad_clicks`, `page_views`
- Dead-letter topic: `dead_letter`
- Sink: Postgres `attributed_page_views`, keyed by `page_view_id`
- Join key: `user_id`; both input topics must be co-partitioned by this key
- Attribution window: latest click where `page_view_time - 30 minutes <= click_time <= page_view_time`
- Allowed lateness: configurable, capped at 15 minutes by `WatermarkTracker`
- Output strategy: immediate UPSERT plus correction UPDATEs
- Offset strategy: page-view offsets become done after durable write; click offsets become done only after watermark-based click eviction, except duplicate logical clicks from a different Kafka source offset may commit after their idempotent correction is flushed

## Join semantics

For each page view, the selected click is the latest click for the same user where:

```text
page_view_time - 30 minutes <= click_time <= page_view_time
```

If no click matches at page-view arrival, nullable attribution fields are written. A later accepted click can update that row if it is newer than the row's current `attributed_click_time`; ties use `click_id` ordering for deterministic behavior.

Clicks are stored in memory by `(user_id, event_time, click_id)`. The retained state entry's Kafka `(partition, offset)` remains replayable until watermark eviction. Duplicate or retried clicks still re-run the database correction. A retry from the retained source offset stays uncommitted; a duplicate logical click from a different source offset may become done after that correction is flushed because the retained offset still protects restart recovery.


### Watermarks and late data

Watermarks are tracked per `(topic, partition)`. The joined watermark for numeric partition `P` is the monotonic minimum of `ad_clicks-P` and `page_views-P`, excluding only sources that Spring Kafka reports as idle, caught up to broker end offset, and unpaused.

Events at or before `joinedWatermark - allowedLateness` are synchronously written to the `dead_letter` topic and then marked commit-safe. Paused or backlogged partitions are never treated as idle, so backlog is not silently discarded as late.

## Output and delivery guarantees

The processor provides at-least-once processing with idempotent visible Postgres output.

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

### Concurrency model

Spring Kafka runs concurrent listeners. Both topics for the same numeric partition are serialized through one partition lock before touching join state or that partition's Postgres sink. Different partitions run in parallel, and each partition has its own sink connection.

## Partitioning and Horizontal Scaling

### Co-partitioning requirement
The join key is `user_id`. Both topics must have the same partition count.
`hash(user_id) % N` must yield the same partition number in both topics.
If partition counts differ, a user's click and page view land on different
partitions — the processor never sees both together — attribution is silently
lost.

### Click state: in-memory vs. shared store

| Approach | Pros | Cons |
|---|---|---|
| In-memory per partition | Zero lookup latency. No extra infra. | Cannot scale horizontally — any instance can only serve its assigned partitions. |
| Shared store (Redis, DB) | Any instance can read any partition's clicks. Horizontal scaling is trivial — add instances, Kafka rebalances partitions. | Extra network hop per lookup. More infra to operate. |

This project's `ClickStateStore` stub provided manages clicks in memory per partition, therefor an in mem solution was selected.

### Scaling up (adding partitions)

Expanding partitions changes `N` in `hash(user_id) % N`, remapping every user.
The processor requires strict co-partitioning, so expansion requires a
controlled migration:

1. Choose a new partition count. Both topics must use the same number.
2. Producers must start producing to the new topics with the new count.
   Migration options:
   - **Dual-write:** producers write to both old and new topics, then the
     old topics are drained after confirmation that all producers switched.
     Expect duplicate events on the new topics — the correction UPDATE and
     the sink's idempotent UPSERT handle these.
   - **Parallel clusters:** run a second cluster with the new topics and
     the new processor config. Cut traffic after validation.
3. Let the processor drain the old topics — verify via dashboard that the
   joined watermark has advanced and no uncommitted offsets remain.
4. Update `kafka.consumer.concurrency` and topic names in `application.yml`.
5. Restart the processor against the new topics.

### Recommendation
Pick partition count upfront: `max(expected_instances × 3, 12)`.
Over-partitioning costs little (one JDBC connection per partition).
Under-partitioning requires the migration above. The default of 3 partitions
is fine for this demo.


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

### Run

Two ways to run. Both expose the dashboard at `http://localhost:8081/dashboard.html`
and Kafka UI at `http://localhost:8080`.

#### Option A — everything in Docker (recommended)

Builds the processor image and runs it alongside Kafka and Postgres on the compose network:

```bash
docker compose up -d --build
```

The `app` service waits for Kafka and Postgres to be healthy, then starts. It uses the
in-container hostnames (`kafka:29092`, `postgres:5432`) from the base `application.yml` —
no extra configuration. Tail it with:

```bash
docker compose logs -f app
```

#### Option B — processor on the host (for IDE / fast iteration)

Start only the infrastructure in Docker, run the processor from the host JVM:

```bash
docker compose up -d zookeeper kafka kafka-ui postgres
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

The `local` profile points the processor at the host-mapped ports (`localhost:9092`,
`localhost:5433`). Do not run Option A and Option B at the same time — both bind host port 8081.

#### Generate sample data

```bash
python data_generator.py        # deterministic edge-case dataset
./stress-test.sh                # high-rate load test (stack + processor must be up)
```

Ports: dashboard `8081`, Kafka UI `8080`, Kafka `9092` (host) / `29092` (in-network),
Postgres `5433` (host) / `5432` (in-network).

### Verification

Run the test suite:

```bash
mvn test
```

Tests using Testcontainers require Docker socket access. If Docker API compatibility is needed:

```bash
sg docker -c 'mvn clean test -Dapi.version=1.40'
```

Inspect attributed output:

```bash
psql -h localhost -p 5433 -U streamprocessor -d attribution -c 'SELECT * FROM attributed_page_views ORDER BY page_view_id'
```

Inspect late-event dead letters:

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic dead_letter --from-beginning
```

Run the stress test after the stack and processor are up:

```bash
./load-test.sh
```

Detailed design notes are in [`DESIGN.md`](DESIGN.md) and [`documentation.md`](documentation.md).

---

## Welcome!

You have received your first challenge to become a part of an awesome team!

**A note about AI tools:** Using AI is not forbidden, but keep in mind this challenge is designed to understand your capabilities. The more you rely on AI, the less we'll understand your real code skills, problem-solving approach, and architectural thinking. We encourage you to use AI as a productivity tool, but make sure the core design decisions and implementation logic reflect your own understanding.

---

## Challenge Overview

Implement a **stream processor** that consumes two event streams and produces an output stream with joined data. This challenge will demonstrate your ability to handle **stream joins, real-time data, delivery guarantees, and concurrency** in a production-like streaming system.

### What You'll Build

A stream processing system that joins two event streams:

**Stream A: `page_view` events**
- `user_id`, `event_time`, `url`, `event_id`

**Stream B: `ad_click` events**
- `user_id`, `event_time`, `campaign_id`, `click_id`

**Goal:** For each `page_view`, attach the **most recent** `ad_click` for the same `user_id` that happened within **30 minutes** prior to `page_view` event in **event time**, and emit new `attributed_page_view` event with fields:

- `page_view_id`, `page_id`, `user_id`, `event_time`, `url`, `attributed_campaign_id` (nullable), `attributed_click_id` (nullable)

### Example Flow

```
User clicks ad for campaign X (12:00) → User views page (12:01) → Output: Page view attributed to ad campaign X
User views page (12:05) → No prior click for that user on that page → Output: Page view with nulls in `attributed_campaign_id` and `attributed_click_id`
```

---

## Important: Repository Naming

**To avoid this challenge being tracked or indexed online, you must name your repository as a car brand and model.**

Examples: `tesla-model-s`, `honda-civic`, `ford-mustang`, `toyota-camry`

**Do not** use terms like "challenge", "interview", "stream-processor", etc. in your repository name.

---

## Requirements

### 1) Stream Join + Out-of-Order Handling

* Events arrive **out of order** (with respect to `event_time`)
* You must support **watermarks**:
  * Configurable allowed lateness (max 15 minutes)
* A `page_view` can be emitted when you are confident no other `ad_click` will arrive that should supersede attribution (based on watermark), **or** you can emit updates to already produced `attributed_page_view` (but then specify your update strategy clearly).

**Edge cases to handle:**

* Click arrives after its page_view (but still within lateness)
* Multiple clicks in the 30-min window → pick the latest click
* Duplicate clicks in the 30-min window → emit only one attributed page view
* Late events beyond allowed lateness → drop or dead-letter (must be explicit)

### 2) Offset Management & Delivery Guarantees

* Input streams provide offsets (like Kafka):
  * `partition`, `offset`, `payload`
* You must implement a **consumer loop** that:
  * Consumes events from both streams
  * Implements join logic according to above spec
  * Writes output to a sink (local file or local DB) and commits consumed offsets to the stream
* Your implementation must be resilient to crashes and restarts. Document your delivery guarantees and what happens under different failure scenarios.

### 3) Concurrency

To achieve high throughput, your implementation should use multiple workers/threads to process partitions and/or batches concurrently. Make sure your implementation is thread-safe and works correctly under concurrent load.

### 5) Determinism + Testability

Provide a deterministic test harness:

* Feed a fixed set of events with known ordering and offsets
* Assert output records exactly (including attribution correctness)
* Include restart test: process half, crash, restart, ensure correctness

---

## Input/Output Contract

### Input

Two streams that yield records:

```
(topic, partition, offset, payload_json)
```

### Output

A sink interface:

```
write(record)
```

Offset commit interface:

```
commit(topic, partition, offset)
```
---

## Deliverables

1. **Working processor implementation** (language of choice: Java or Scala)
2. **Documentation explaining:**
   * Watermark logic
   * Write semantics (emit-once vs update)
   * Delivery guarantees (at-least-once, exactly-once, idempotence, etc.) and failure modes
   * Concurrency model
   * Capacity planning and scaling (state size, number of workers/instances)
3. **Tests for:**
   * Out-of-order events
   * Late data
   * Restart with committed offsets
   * Concurrent partitions
4. **README with:**
   * Setup instructions
   * How to run the processor
   * How to verify results

---

## Bonus Challenge

**Frontend Dashboard (optional, can be AI-generated)**

Create a simple web interface that shows the data flow in real-time:

* Visual representation of incoming events (clicks and page views)
* Current watermark position
* Attribution matches being made
* State size / memory usage
* Processing lag by partition

This can be built with any framework (React, Vue, simple HTML/JS) and can leverage AI code generation tools. The focus is on **visualizing the streaming concepts**, not frontend engineering excellence.

Example features:
* Live event stream visualization
* Attribution timeline view
* Metrics dashboard (throughput, latency, state size)
* Event inspector (click any event to see its journey)

---

## Event Schemas

### Input: Page View Event
```json
{
  "user_id": "user_1",
  "event_time": "2024-01-01T12:10:00Z",
  "url": "https://example.com/product",
  "event_id": "pv_1"
}
```

### Input: Ad Click Event
```json
{
  "user_id": "user_1",
  "event_time": "2024-01-01T12:00:00Z",
  "campaign_id": "campaign_A",
  "click_id": "click_1"
}
```

### Output: Attributed Page View
```json
{
  "page_view_id": "pv_1",
  "user_id": "user_1",
  "event_time": "2024-01-01T12:10:00Z",
  "url": "https://example.com/product",
  "attributed_campaign_id": "campaign_A",
  "attributed_click_id": "click_1"
}
```

## Test Scenarios

The data generator creates events covering these edge cases:

| Scenario | Description | Expected Behavior |
|----------|-------------|-------------------|
| **Normal** | Click at 12:00, page view at 12:10 | Attributes to campaign |
| **Out-of-order** | Page view arrives before its click | Handles via watermarks |
| **Multiple clicks** | 2+ clicks in 30-min window | Attributes to most recent |
| **Old click** | Click >30 minutes before page view | No attribution |
| **Late event** | Event beyond allowed lateness (2 min) | Dropped |
| **No click** | Page view with no prior click | Null attribution |

---

## Extension Ideas (Optional)

If you finish the core requirements and want to go further, consider these extensions:

* **Backpressure:** Simulate slow sink and ensure consumers don't OOM
* **Skew handling:** Hot users / hot partitions—how to mitigate contention
* **Metrics & Monitoring:** Add instrumentation to track processing latency, throughput, and state size

---

## Learning Resources

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spring Kafka Reference](https://spring.io/projects/spring-kafka)
- [Streaming Systems Book](http://streamingsystems.net/)
- [The Dataflow Model Paper](https://research.google/pubs/pub43864/)

---

## Questions?

If you have questions about the requirements, edge cases, or technical setup, please reach out to your contact person.

---

**Good luck, and we're excited to see your solution! 🚀**
