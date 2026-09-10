"""Order consumer: real-time aggregation with retries and a dead letter queue.

Run it with ``order-consumer`` (or ``python -m order_pipeline.apps.consumer``).

Per message the consumer:

1. decodes the Avro payload - a failure here is permanent, so the raw bytes go
   straight to the DLQ;
2. validates the business rules - also permanent, straight to the DLQ;
3. delivers the order downstream, retrying transient failures with exponential
   backoff until the retry budget is spent, then dead-letters it;
4. folds a successful order into the running average and, periodically,
   publishes that snapshot to the aggregates topic.

**Delivery semantics.** Offsets are stored only after a message reaches a
terminal state (processed *or* dead-lettered) and are committed by librdkafka
shortly afterwards, which gives at-least-once processing: a crash mid-message
replays that message rather than losing it.
"""

from __future__ import annotations

import argparse
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)

from order_pipeline.aggregation import OrderAggregator
from order_pipeline.config import AppSettings, ConsumerSettings, KafkaSettings
from order_pipeline.dlq import DeadLetterPublisher
from order_pipeline.errors import (
    DeserializationError,
    PermanentProcessingError,
    ProcessingError,
    RetryExhaustedError,
)
from order_pipeline.logging_config import KAFKA_CLIENT_LOGGER, configure_logging
from order_pipeline.models import Order
from order_pipeline.processing import FlakyDownstream, OrderProcessor
from order_pipeline.retry import RetryPolicy
from order_pipeline.serdes import (
    MessageField,
    SerializationContext,
    build_schema_registry_client,
    key_serializer,
    order_aggregate_serializer,
    order_deserializer,
)
from order_pipeline.shutdown import ShutdownSignal

logger = logging.getLogger("order_pipeline.consumer")

POLL_TIMEOUT_S = 1.0


@dataclass(slots=True)
class ConsumerCounters:
    """Tally shown in the final report."""

    consumed: int = 0
    processed: int = 0
    retried: int = 0
    dead_lettered: int = 0


def _describe(partitions: list[TopicPartition]) -> str:
    """Render a partition list as ``topic[0], topic[1]``.

    TopicPartition's own repr is built by librdkafka with C format specifiers
    that Windows does not expand, so it prints literally as
    ``partition=%I32d,offset=%s``. Formatting the fields here keeps the
    rebalance log readable on every platform.
    """
    return ", ".join(f"{tp.topic}[{tp.partition}]" for tp in partitions) or "none"


def build_consumer(kafka: KafkaSettings, settings: ConsumerSettings) -> Consumer:
    """Create the consumer with manual offset *storing*.

    ``enable.auto.offset.store=False`` is the important flag: auto-commit stays
    on (cheap, batched), but an offset only becomes eligible for commit once
    this code has explicitly stored it after handling the message. Without it,
    librdkafka would commit offsets for messages still in flight and a crash
    would silently drop them.
    """
    return Consumer(
        {
            "bootstrap.servers": kafka.bootstrap_servers,
            "group.id": settings.group_id,
            "client.id": "ee8202-order-consumer",
            "auto.offset.reset": settings.auto_offset_reset,
            "enable.auto.commit": True,
            "enable.auto.offset.store": False,
            "auto.commit.interval.ms": 5_000,
            "session.timeout.ms": 45_000,
            "max.poll.interval.ms": 300_000,
            "partition.assignment.strategy": "cooperative-sticky",
        },
        logger=logging.getLogger(KAFKA_CLIENT_LOGGER),
    )


def build_side_producer(kafka: KafkaSettings) -> Producer:
    """One producer serves both the DLQ and the aggregates topic."""
    return Producer(
        {
            "bootstrap.servers": kafka.bootstrap_servers,
            "client.id": "ee8202-order-consumer-egress",
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "delivery.timeout.ms": 60_000,
        },
        logger=logging.getLogger(KAFKA_CLIENT_LOGGER),
    )


class AggregatePublisher:
    """Emits running-average snapshots to the aggregates topic.

    Publication is rate-limited by both a message count and a wall-clock
    interval so a fast stream does not drown the topic and a slow one still
    produces a heartbeat.
    """

    def __init__(
        self,
        producer: Producer,
        topic: str,
        serializer: Callable[[Any, SerializationContext], bytes | None],
        every_n: int,
        interval_s: float,
        enabled: bool = True,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._serialize = serializer
        self._every_n = every_n
        self._interval_s = interval_s
        self._enabled = enabled
        self._since_last = 0
        self._last_emit = time.monotonic()
        self._serialize_key = key_serializer()
        self._value_ctx = SerializationContext(topic, MessageField.VALUE)
        self._key_ctx = SerializationContext(topic, MessageField.KEY)

    def note_processed(self) -> None:
        self._since_last += 1

    def due(self) -> bool:
        return self._since_last >= self._every_n or (
            self._since_last > 0 and time.monotonic() - self._last_emit >= self._interval_s
        )

    def emit(self, aggregator: OrderAggregator, force: bool = False) -> None:
        """Log the running average and publish one record per scope."""
        if not force and not self.due():
            return
        if aggregator.count == 0:
            return

        self._since_last = 0
        self._last_emit = time.monotonic()
        logger.info("AGGREGATE  %s", aggregator.summary_line())

        if not self._enabled:
            return

        emitted_at = int(time.time() * 1000)

        for record in aggregator.snapshots():
            payload = dict(record, emittedAt=emitted_at)
            try:
                self._producer.produce(
                    topic=self._topic,
                    key=self._serialize_key(str(record["scope"]), self._key_ctx),
                    value=self._serialize(payload, self._value_ctx),
                )
            except BufferError:  # pragma: no cover - back-pressure safety valve
                logger.warning("aggregate queue is full; dropping this snapshot")
                self._producer.poll(0)
                return
        self._producer.poll(0)


class OrderConsumerApp:
    """Wires the Kafka plumbing to the pure processing core."""

    def __init__(
        self,
        kafka: KafkaSettings,
        settings: ConsumerSettings,
        shutdown: ShutdownSignal,
    ) -> None:
        self._kafka = kafka
        self._settings = settings
        self._shutdown = shutdown
        self.counters = ConsumerCounters()
        self.aggregator = OrderAggregator()

        registry = build_schema_registry_client(kafka)
        self._deserialize_order = order_deserializer(registry)
        self._consumer = build_consumer(kafka, settings)
        self._egress = build_side_producer(kafka)
        self._dlq = DeadLetterPublisher(self._egress, kafka.dlq_topic, settings.group_id)
        self._aggregates = AggregatePublisher(
            self._egress,
            kafka.aggregates_topic,
            order_aggregate_serializer(registry),
            settings.aggregate_emit_every_n,
            settings.aggregate_emit_interval_s,
            settings.publish_aggregates,
        )
        self._processor = OrderProcessor(
            aggregator=self.aggregator,
            sink=FlakyDownstream(settings.transient_failure_rate),
            retry_policy=RetryPolicy(
                max_attempts=settings.max_retry_attempts,
                initial_backoff_s=settings.retry_initial_backoff_s,
                multiplier=settings.retry_backoff_multiplier,
                max_backoff_s=settings.retry_max_backoff_s,
                jitter_ratio=settings.retry_jitter_ratio,
            ),
            rng=random.Random(),
        )

    def run(self) -> ConsumerCounters:
        """Poll, process and aggregate until a shutdown is requested."""
        topic = self._kafka.orders_topic
        self._consumer.subscribe([topic], on_assign=self._on_assign, on_revoke=self._on_revoke)
        logger.info(
            "consuming '%s' as group '%s' (retries=%d, dlq='%s')",
            topic,
            self._settings.group_id,
            self._settings.max_retry_attempts,
            self._kafka.dlq_topic,
        )

        try:
            while not self._shutdown.requested:
                message = self._consumer.poll(POLL_TIMEOUT_S)
                if message is None:
                    self._aggregates.emit(self.aggregator)
                    continue
                if message.error():
                    self._handle_consumer_error(message)
                    continue

                self.counters.consumed += 1
                self._handle_message(message)
                # Terminal state reached: the offset may now advance.
                self._store_offset(message)
                self._aggregates.emit(self.aggregator)
        finally:
            self._close()

        return self.counters

    def _handle_message(self, message: Message) -> None:
        """Decode, process and, on failure, dead-letter a single message."""
        try:
            order = self._decode(message)
        except DeserializationError as exc:
            self.counters.dead_lettered += 1
            self._dlq.publish(message, exc)
            return

        try:
            outcome = self._processor.handle(order)
        except RetryExhaustedError as exc:
            self.counters.dead_lettered += 1
            self._dlq.publish(message, exc, attempts=exc.attempts)
        except PermanentProcessingError as exc:
            self.counters.dead_lettered += 1
            self._dlq.publish(message, exc)
        except ProcessingError as exc:  # pragma: no cover - defensive
            self.counters.dead_lettered += 1
            self._dlq.publish(message, exc)
        else:
            self.counters.processed += 1
            if outcome.was_retried:
                self.counters.retried += 1
            self._aggregates.note_processed()
            logger.info(
                "processed %-6s %-8s %8.2f (attempts=%d) | %s",
                order.order_id,
                order.product,
                order.price,
                outcome.attempts,
                self.aggregator.summary_line(),
            )

    def _decode(self, message: Message) -> Order:
        """Turn message bytes into an :class:`Order`.

        Every decode failure is normalised into :class:`DeserializationError`
        so the DLQ routing rule has a single type to match on.
        """
        ctx = SerializationContext(message.topic() or "", MessageField.VALUE)
        try:
            record = self._deserialize_order(message.value(), ctx)
        except Exception as exc:  # any decode failure is terminal - re-raised below
            raise DeserializationError(f"could not decode Avro payload: {exc}") from exc

        if record is None:
            raise DeserializationError("message value is null (tombstone)")
        if not isinstance(record, dict):
            raise DeserializationError(f"expected an Avro record, got {type(record).__name__}")
        try:
            return Order.from_avro(record)
        except PermanentProcessingError as exc:
            raise DeserializationError(str(exc)) from exc

    def _store_offset(self, message: Message) -> None:
        """Mark the message as done so its offset becomes eligible for commit.

        A cooperative rebalance can revoke the partition while the message was
        being processed, in which case librdkafka rejects the store. That is
        not fatal: whoever now owns the partition will replay the message,
        which at-least-once already allows for.
        """
        try:
            self._consumer.store_offsets(message=message)
        except KafkaException as exc:
            logger.warning(
                "could not store the offset for %s[%d]@%d (partition likely revoked): %s",
                message.topic(),
                message.partition(),
                message.offset(),
                exc,
            )

    def _handle_consumer_error(self, message: Message) -> None:
        error = message.error()
        if error is None:  # pragma: no cover - only called when there is an error
            return
        if error.code() == KafkaError._PARTITION_EOF:  # noqa: SLF001 - documented constant
            logger.debug("reached end of %s[%d]", message.topic(), message.partition())
            return
        # A fatal error means the client is unusable; anything else is a
        # transport hiccup librdkafka recovers from on its own.
        if error.fatal():
            raise KafkaException(error)
        logger.warning("recoverable consumer error: %s", error)

    def _on_assign(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        logger.info("assigned %d partition(s): %s", len(partitions), _describe(partitions))

    def _on_revoke(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        logger.info("revoking %d partition(s): %s", len(partitions), _describe(partitions))

    def _close(self) -> None:
        """Emit a final snapshot, flush the egress producer, commit and leave."""
        logger.info("shutting down...")
        self._aggregates.emit(self.aggregator, force=True)
        self._egress.flush(timeout=15.0)
        # close() commits the stored offsets and leaves the group cleanly, so a
        # restart does not wait out the session timeout before rebalancing.
        self._consumer.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="order-consumer",
        description="Consume Avro orders, aggregate prices, retry and dead-letter failures.",
    )
    parser.add_argument("--group", help="consumer group id")
    parser.add_argument("--topic", help="override the orders topic")
    parser.add_argument("--max-retries", type=int, help="total attempts per message")
    parser.add_argument(
        "--failure-rate",
        type=float,
        help="simulated transient failure rate of the downstream sink [0-1]",
    )
    parser.add_argument(
        "--from-beginning", action="store_true", help="read the topic from offset 0"
    )
    parser.add_argument(
        "--no-publish-aggregates",
        action="store_true",
        help="log the running average but do not publish it to Kafka",
    )
    parser.add_argument("--log-level", help="DEBUG, INFO, WARNING, ...")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    app = AppSettings()
    configure_logging(args.log_level or app.log_level)

    kafka = KafkaSettings()
    if args.topic:
        kafka.orders_topic = args.topic

    overrides = {
        "group_id": args.group,
        "max_retry_attempts": args.max_retries,
        "transient_failure_rate": args.failure_rate,
    }
    settings = ConsumerSettings(**{k: v for k, v in overrides.items() if v is not None})
    if args.from_beginning:
        settings.auto_offset_reset = "earliest"
    if args.no_publish_aggregates:
        settings.publish_aggregates = False

    shutdown = ShutdownSignal().install()
    counters = OrderConsumerApp(kafka, settings, shutdown).run()

    logger.info(
        "consumer finished - consumed=%d processed=%d (of which retried=%d) dead_lettered=%d",
        counters.consumed,
        counters.processed,
        counters.retried,
        counters.dead_lettered,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
