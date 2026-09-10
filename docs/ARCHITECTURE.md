# Architecture and design decisions

This document records *why* the system is built the way it is. The code explains
what happens; this explains what the alternatives were and why they were rejected.

## 1. Layering

```
              ┌─────────────────────────────────────────────┐
  apps/       │  producer.py   consumer.py   dlq_inspector   │  Kafka I/O, CLI, signals
              ├─────────────────────────────────────────────┤
  adapters    │  serdes.py     dlq.py        config.py       │  Confluent clients, settings
              ├─────────────────────────────────────────────┤
  domain      │  models.py  processing.py  retry.py          │  no Kafka import anywhere
              │  aggregation.py            errors.py         │
              └─────────────────────────────────────────────┘
```

The domain layer imports nothing from `confluent_kafka`. That is not decoration:
it is what makes the retry budget, the backoff curve, the validation rules and
the running average testable in milliseconds with a fake sink and a fake clock,
instead of needing a broker and a stopwatch. `tests/` runs with no infrastructure
at all.

## 2. Message flow

1. **Producer** builds an `Order`, serialises it with the Avro serialiser (which
   registers `order.avsc` under the subject `orders-value` on first use), keys it
   by `orderId` and publishes to `orders`.
2. **Consumer** polls, decodes, validates, delivers to a downstream sink with
   retries, and folds successes into the running average.
3. Anything that cannot be made to succeed is copied to `orders.DLQ`.
4. Snapshots of the running average are published to `orders.aggregates`.

## 3. Decisions

### 3.1 Avro schema read from disk, never inlined

`schemas/order.avsc` is read at runtime by `schemas.load_schema()`. The obvious
alternative — a Python string literal — creates two sources of truth: the file
submitted with the assignment and the schema actually registered. They drift the
first time someone edits one and not the other. A unit test
(`test_schemas.py`) additionally asserts the file still matches the brief exactly:
three fields, `orderId: string`, `product: string`, `price: float`.

### 3.2 Errors are classified, not caught by type

`errors.py` defines a two-branch taxonomy — `TransientProcessingError` and
`PermanentProcessingError` — and every library exception is normalised into one
of them at the boundary. The routing rule is then a single statement that fits in
a sentence:

> transient → retry with backoff, then dead-letter; permanent → dead-letter now.

Without this, retry decisions get scattered across `except` clauses for whichever
third-party exception happened to be observed during development, and the
behaviour for a *new* exception type is undefined. Here, an unrecognised
exception is dead-lettered with reason `UNEXPECTED_ERROR` rather than silently
retried forever.

**Permanent errors must fail fast.** Retrying a message with a negative price
four times, with backoff, does not fix the price — it just blocks that partition
for several seconds and delays every message behind it.

### 3.3 In-consumer retry, not a retry topic

Two standard patterns exist:

| | In-consumer retry (chosen) | Retry topics (`orders.retry.5s`, …) |
|---|---|---|
| Ordering | preserved per partition | lost — retried messages jump the queue |
| Head-of-line blocking | yes, during the backoff | avoided |
| Complexity | one loop | N extra topics, N consumers, a scheduler |

For this workload — a bounded retry budget with a sub-second-to-seconds backoff,
and prices that are aggregated in order — head-of-line blocking for at most a few
seconds is a fair price for keeping ordering and keeping the design small. A
retry-topic ladder is the right answer when backoffs run into minutes or hours;
it would be over-engineering here. The `max.poll.interval.ms` of 5 minutes gives
ample headroom over the worst-case total backoff (~9 s at the defaults), so a
retrying consumer is never mistaken for a dead one and evicted from the group.

### 3.4 Jitter on the backoff

`RetryPolicy` randomises each delay by ±20 % by default. With a fixed backoff, a
downstream that recovers on a timer receives every retry of every in-flight
message at exactly the same instant and is knocked over again. Jitter spreads
them out. It costs one line and one test.

### 3.5 The DLQ carries the original bytes; context goes in headers

The dead-lettered value is the source message's value, copied verbatim.

* Re-serialising is *impossible* for the most important case — a payload that
  could not be decoded is exactly what the DLQ exists to capture.
* Wrapping the payload in an envelope record would make the DLQ topic
  incompatible with the source topic, so replaying a fixed message would require
  an unwrapping step.

Instead the failure context travels as Kafka headers (`x-dlq-reason`,
`x-dlq-error-class`, `x-dlq-error-message`, `x-dlq-attempts`, `x-origin-topic`,
`x-origin-partition`, `x-origin-offset`, …). Replay is then a byte copy back to
`orders`. Stale `x-dlq-*` headers are stripped on the way in, so a message that
is replayed and fails twice does not accumulate contradictory context.

The DLQ topic has **one partition** (strict arrival order for a low-volume,
human-read topic) and a **30-day retention** against the source topic's 7 days —
a dead letter is evidence, and it should outlive the message that caused it.

### 3.6 Delivery semantics: at-least-once, deliberately

The consumer sets `enable.auto.commit=true` but `enable.auto.offset.store=false`,
and calls `store_offsets()` only after a message reaches a terminal state
(processed *or* dead-lettered). Committing is then batched and cheap, but no
offset is ever committed for a message still in flight.

`DeadLetterPublisher.publish()` **flushes** before returning, so the ordering is:

```
dlq write acknowledged by the broker  →  offset stored  →  offset committed
```

A crash anywhere in that sequence replays the message. The consequence is
duplicate processing on recovery — accepted, because the alternative
(commit-then-process) loses messages, and losing an order is worse than counting
one twice. Exactly-once would need a transactional producer with
`send_offsets_to_transaction`; that is real complexity, and the assignment's
aggregation does not warrant it.

### 3.7 Welford's algorithm for the running average

`RunningStats.update()` maintains the mean incrementally rather than dividing a
running total. Both are O(1) per message, but on a long-lived stream the naive
total grows far larger than any individual price and loses low-order bits.
Welford keeps the error bounded. `test_aggregation.py` pins both properties: it
matches a plain average on 10 000 random prices, and stays accurate where a naive
sum would not.

Only *successfully processed* orders are aggregated, so the running average never
includes a message that was dead-lettered.

### 3.8 Idempotent producers

Both the order producer and the consumer's egress producer set
`enable.idempotence=true` with `acks=all`. Without it, librdkafka's internal
retry on a network blip can silently duplicate or reorder a record — which would
corrupt the very average the system exists to compute.

### 3.9 Failures are injected, not waited for

A live demonstration cannot wait for a real network partition. The producer
injects two classes of poison message (`--invalid-rate`, `--corrupt-rate`) and
the consumer's `FlakyDownstream` fails a configurable share of deliveries
(`--failure-rate`). Every rate defaults to a non-zero value, so retries and dead
letters appear within seconds of starting the demo, and every rate can be set to
`0` for a clean run.

## 4. Topics

| Topic | Partitions | Retention | Key | Purpose |
|---|---|---|---|---|
| `orders` | 3 | 7 days | `orderId` | the order stream |
| `orders.DLQ` | 1 | 30 days | original key | permanently failed messages |
| `orders.aggregates` | 1 | 1 day | scope (`ALL` / product) | running-average snapshots |

Broker auto-creation is **disabled**; `scripts/create-topics.sh` creates all three
explicitly. A typo in a topic name should fail loudly rather than quietly create
a new single-partition topic with default settings.

## 5. What a production deployment would change

The shortcuts taken here are deliberate and localised, and worth naming:

* **Replication factor 1, one broker.** Fine for a laptop; a real cluster needs
  RF≥3 with `min.insync.replicas=2`.
* **`FlakyDownstream` is a simulation.** In production the sink is a real
  service, and the transient/permanent classification would be derived from its
  actual responses (HTTP 5xx vs 4xx, driver error codes).
* **No authentication or TLS.** A real cluster would use SASL/SSL, which is a
  client-configuration change in `build_producer` / `build_consumer` only.
* **`auto.register.schemas=true`.** Convenient for development; production
  registers schemas through CI so an incompatible schema cannot be introduced by
  deploying an application.
* **No metrics endpoint.** Counters are logged; they would be exported to
  Prometheus, with alerts on the DLQ rate and on consumer lag.
