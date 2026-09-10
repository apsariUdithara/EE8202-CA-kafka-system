"""Order producer: publishes Avro-encoded order messages to Kafka.

Run it with ``order-producer`` (or ``python -m order_pipeline.apps.producer``).

To make the consumer's failure handling demonstrable, a configurable share of
the stream is deliberately poisoned:

* ``--invalid-rate``  valid Avro, invalid business data (negative price, blank
  id) - the consumer dead-letters it immediately, without burning retries;
* ``--corrupt-rate``  bytes that are not Avro at all - the consumer cannot even
  decode it and dead-letters it as a deserialization failure.

Everything else is a well-formed order with a randomised price.
"""

from __future__ import annotations

import argparse
import itertools
import logging
import random
from collections.abc import Callable
from dataclasses import dataclass

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from order_pipeline.config import AppSettings, KafkaSettings, ProducerSettings
from order_pipeline.logging_config import configure_logging
from order_pipeline.models import Order
from order_pipeline.serdes import (
    MessageField,
    SerializationContext,
    build_schema_registry_client,
    key_serializer,
    order_serializer,
)
from order_pipeline.shutdown import ShutdownSignal

logger = logging.getLogger("order_pipeline.producer")

#: Deliberately malformed bytes: a Confluent framing prefix followed by junk.
CORRUPT_PAYLOAD = b"\x00\x00\x00\x00\x00not-an-avro-record"


@dataclass(slots=True)
class ProducerCounters:
    """Tally shown in the final report."""

    valid: int = 0
    invalid: int = 0
    corrupt: int = 0
    delivery_failures: int = 0

    @property
    def total(self) -> int:
        return self.valid + self.invalid + self.corrupt


class OrderFactory:
    """Generates the synthetic order stream."""

    def __init__(self, settings: ProducerSettings, rng: random.Random | None = None) -> None:
        self._settings = settings
        self._rng = rng or random.Random(settings.seed)
        self._ids = itertools.count(1001)

    def next_order_id(self) -> str:
        return str(next(self._ids))

    def valid(self) -> Order:
        """A well-formed order with a randomised price."""
        settings = self._settings
        return Order(
            order_id=self.next_order_id(),
            product=self._rng.choice(settings.products),
            price=round(self._rng.uniform(settings.min_price, settings.max_price), 2),
        )

    def invalid(self) -> Order:
        """An order that satisfies the Avro schema but breaks a business rule."""
        settings = self._settings
        if self._rng.random() < 0.5:
            return Order(
                order_id=self.next_order_id(),
                product=self._rng.choice(settings.products),
                price=-round(self._rng.uniform(1, 99), 2),
            )
        return Order(order_id="", product=self._rng.choice(settings.products), price=42.0)

    def roll(self) -> str:
        """Pick what the next message should be: valid, invalid or corrupt."""
        draw = self._rng.random()
        if draw < self._settings.corrupt_payload_rate:
            return "corrupt"
        if draw < self._settings.corrupt_payload_rate + self._settings.invalid_payload_rate:
            return "invalid"
        return "valid"


def _delivery_callback(counters: ProducerCounters) -> Callable[[KafkaError | None, Message], None]:
    """Build the async delivery report handler for this run."""

    def callback(err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            counters.delivery_failures += 1
            logger.error("delivery failed: %s", err)
        else:
            logger.debug("delivered to %s[%d]@%d", msg.topic(), msg.partition(), msg.offset())

    return callback


def build_producer(settings: KafkaSettings) -> Producer:
    """Create an idempotent, durability-first producer.

    ``enable.idempotence`` together with ``acks=all`` means a retry inside
    librdkafka cannot silently duplicate or reorder a record - the guarantee
    the downstream aggregation depends on.
    """
    return Producer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "client.id": "ee8202-order-producer",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 20,
            "retries": 5,
            "retry.backoff.ms": 200,
            "delivery.timeout.ms": 120_000,
        }
    )


def run(
    kafka: KafkaSettings,
    producer_settings: ProducerSettings,
    shutdown: ShutdownSignal,
) -> ProducerCounters:
    """Publish orders until the message budget is spent or a signal arrives."""
    registry = build_schema_registry_client(kafka)
    serialize_order = order_serializer(registry)
    serialize_key = key_serializer()
    producer = build_producer(kafka)
    factory = OrderFactory(producer_settings)
    counters = ProducerCounters()
    on_delivery = _delivery_callback(counters)

    value_ctx = SerializationContext(kafka.orders_topic, MessageField.VALUE)
    key_ctx = SerializationContext(kafka.orders_topic, MessageField.KEY)
    interval = 1.0 / producer_settings.message_rate_per_second
    budget = producer_settings.max_messages or None

    logger.info(
        "producing to '%s' at %.2f msg/s (invalid=%.0f%%, corrupt=%.0f%%, budget=%s)",
        kafka.orders_topic,
        producer_settings.message_rate_per_second,
        producer_settings.invalid_payload_rate * 100,
        producer_settings.corrupt_payload_rate * 100,
        budget or "unlimited",
    )

    try:
        while not shutdown.requested and (budget is None or counters.total < budget):
            kind = factory.roll()
            payload: bytes | None

            if kind == "corrupt":
                message_key = factory.next_order_id()
                payload = CORRUPT_PAYLOAD
                counters.corrupt += 1
                logger.warning("injecting CORRUPT payload with key %s", message_key)
            else:
                order = factory.invalid() if kind == "invalid" else factory.valid()
                message_key = order.order_id
                payload = serialize_order(order, value_ctx)
                if kind == "invalid":
                    counters.invalid += 1
                    logger.warning(
                        "injecting INVALID order id=%r product=%r price=%.2f",
                        order.order_id,
                        order.product,
                        order.price,
                    )
                else:
                    counters.valid += 1
                    logger.info("order %-6s %-8s %8.2f", order.order_id, order.product, order.price)

            producer.produce(
                topic=kafka.orders_topic,
                key=serialize_key(message_key, key_ctx),
                value=payload,
                on_delivery=on_delivery,
            )
            # Serve delivery callbacks without blocking, then pace the stream.
            producer.poll(0)
            shutdown.wait(interval)
    except KafkaException:
        logger.exception("producer aborted")
        raise
    finally:
        logger.info("flushing %d queued message(s)...", len(producer))
        remaining = producer.flush(timeout=30.0)
        if remaining:
            logger.error("%d message(s) were not delivered before shutdown", remaining)

    return counters


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="order-producer",
        description="Publish Avro-encoded order messages to Kafka.",
    )
    parser.add_argument("--rate", type=float, help="messages per second")
    parser.add_argument("--count", type=int, help="stop after N messages (0 = unlimited)")
    parser.add_argument("--invalid-rate", type=float, help="share of business-invalid orders")
    parser.add_argument("--corrupt-rate", type=float, help="share of non-Avro payloads")
    parser.add_argument("--seed", type=int, help="seed the RNG for a reproducible run")
    parser.add_argument("--topic", help="override the orders topic")
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
        "message_rate_per_second": args.rate,
        "max_messages": args.count,
        "invalid_payload_rate": args.invalid_rate,
        "corrupt_payload_rate": args.corrupt_rate,
        "seed": args.seed,
    }
    settings = ProducerSettings(**{k: v for k, v in overrides.items() if v is not None})

    shutdown = ShutdownSignal().install()
    counters = run(kafka, settings, shutdown)

    logger.info(
        "producer finished - %d message(s): %d valid, %d invalid, %d corrupt, %d undelivered",
        counters.total,
        counters.valid,
        counters.invalid,
        counters.corrupt,
        counters.delivery_failures,
    )
    return 1 if counters.delivery_failures else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
