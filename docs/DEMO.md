# Live demonstration script

Roughly 10 minutes. Every command is copy-pasteable. Have four terminals open in
the repository root, plus a browser at <http://localhost:8080>.

Activate the virtual environment in each terminal first
(`.venv\Scripts\Activate.ps1` on Windows, `source .venv/bin/activate` elsewhere).

---

## 0. Before the audience arrives

```bash
docker compose up -d
docker compose ps          # wait until kafka and schema-registry are "healthy"
pytest -q                  # green, and needs no broker
```

Reset to a clean slate if you have rehearsed already:

```bash
docker compose down -v && docker compose up -d
```

---

## 1. The stack (1 min)

> "One Kafka broker in KRaft mode — no ZooKeeper — a Confluent Schema Registry,
> and the three topics the pipeline uses. Auto-creation is off, so the topics are
> created explicitly by a bootstrap script."

```bash
docker compose ps
docker compose logs kafka-init | tail -20
```

Show **Kafka UI → Topics**: `orders` (3 partitions), `orders.DLQ`, `orders.aggregates`.

---

## 2. Avro, and where the schema lives (1 min)

```bash
cat schemas/order.avsc
```

> "Three fields, exactly as the brief specifies. This file is the single source
> of truth — it is read at runtime and registered with the Schema Registry, so
> the registered schema cannot drift from the file in the repository. A unit test
> asserts it still matches the brief."

Run the producer for a moment (step 3), then show the schema was registered:

```bash
curl -s http://localhost:8081/subjects
curl -s http://localhost:8081/subjects/orders-value/versions/1 | python -m json.tool
```

Or point at **Kafka UI → Schema Registry**.

---

## 3. The happy path and the running average (2 min)

**Terminal A — consumer:**

```bash
order-consumer --failure-rate 0
```

**Terminal B — producer, clean traffic:**

```bash
order-producer --rate 4 --invalid-rate 0 --corrupt-rate 0 --seed 42
```

> "Every message is Avro-encoded, keyed by `orderId`. The consumer decodes it,
> validates it, delivers it downstream and folds the price into a running
> average — updated per message, in O(1) memory, using Welford's algorithm. It
> keeps a global average and a per-product breakdown."

Point at the `running avg=…` on each processed line, and at the periodic
`AGGREGATE` line.

Show the aggregates arriving in Kafka as Avro too — **Kafka UI → Topics →
`orders.aggregates` → Messages**.

Stop both with `Ctrl+C`, and note the graceful shutdown: the producer flushes,
the consumer emits a final snapshot and commits its offsets.

---

## 4. Retry logic (2 min)

**Terminal A:**

```bash
order-consumer --failure-rate 0.4 --max-retries 4
```

**Terminal B:**

```bash
order-producer --rate 2 --invalid-rate 0 --corrupt-rate 0
```

> "The downstream now fails 40 % of deliveries with a *transient* error. Watch the
> retry lines: the backoff doubles — roughly 0.2 s, 0.4 s, 0.8 s — with ±20 %
> jitter so that a recovering downstream doesn't get every retry at the same
> instant. Most messages succeed on the second or third attempt, and the
> `attempts=` counter shows it."

Point at a line like:

```
order 1007 failed transiently on attempt 1/4 (...); retrying in 0.18s
order 1007 failed transiently on attempt 2/4 (...); retrying in 0.43s
processed 1007   Item2     153.20 (attempts=3) | running avg=...
```

> "And crucially — a retried message is only counted in the average **once**,
> when it finally succeeds."

---

## 5. The Dead Letter Queue (3 min)

Keep the consumer from step 4 running. **Terminal B — now inject poison:**

```bash
order-producer --rate 3 --invalid-rate 0.2 --corrupt-rate 0.15
```

Three different failures reach the DLQ, and it is worth naming each one:

| What the producer sends | What the consumer does | DLQ reason |
|---|---|---|
| bytes that are not Avro | cannot decode — no point retrying | `DESERIALIZATION_FAILURE` |
| valid Avro, negative price / blank id | fails validation — no point retrying | `VALIDATION_FAILURE` |
| a valid order the flaky sink keeps rejecting | retries 4×, then gives up | `RETRY_EXHAUSTED` |

> "Note that the two permanent failures are **not** retried at all. Retrying a
> negative price four times with backoff doesn't fix the price — it just blocks
> the partition for several seconds."

**Terminal C — inspect the queue:**

```bash
order-dlq
```

```
#3  orders.DLQ[0]@2  key='1014'
  reason   : DESERIALIZATION_FAILURE
  attempts : 1
  error    : could not decode Avro payload: ...
  origin   : orders[1]@9
  payload  : <undecodable, 23 bytes> 00 00 00 00 00 6e 6f 74 ...
--------------------------------------------------------------
6 dead-lettered message(s):
      3  RETRY_EXHAUSTED
      2  DESERIALIZATION_FAILURE
      1  VALIDATION_FAILURE
```

> "The DLQ keeps the **original bytes**, unchanged — re-encoding would be
> impossible for the corrupt payload, which is exactly the case the DLQ exists
> for. The diagnosis travels in Kafka headers instead, so the DLQ topic stays
> byte-compatible with `orders` and a fixed message can be replayed by copying
> its value straight back."

Show the headers in **Kafka UI → Topics → `orders.DLQ` → Messages → Headers**.

---

## 6. Durability (1 min)

> "One thing worth showing: the offset is only stored after a message reaches a
> terminal state, and the DLQ write is flushed *before* that. So a crash replays
> the message rather than losing it — at-least-once."

Kill the consumer mid-stream, restart it, and show it resuming from where it
stopped:

```bash
# Ctrl+C in terminal A, then:
order-consumer
```

Show consumer lag returning to zero in **Kafka UI → Consumers → `order-consumer`**.

---

## 7. Tests and quality gates (1 min)

```bash
pytest -q
ruff check .
```

> "The domain core has no Kafka dependency, so the entire failure matrix —
> retry-then-succeed, retry-exhausted, permanent-fail-fast, and the guarantee that
> a failed order never reaches the average — is covered by unit tests that run in
> about a second with no broker. The same checks run in CI on every push."

---

## Teardown

```bash
docker compose down -v
```

## If something goes wrong on the day

| Symptom | Fix |
|---|---|
| Consumer prints nothing | it is caught up; start the producer, or use `order-consumer --group demo-2` to re-read from the start |
| `Connection refused` | `docker compose ps` — wait for `healthy`, the broker takes ~20 s |
| No DLQ entries yet | the injection is probabilistic — raise the rates: `order-producer --rate 5 --corrupt-rate 0.4` |
| Schema Registry 404 | it starts after Kafka; give it a few more seconds |
