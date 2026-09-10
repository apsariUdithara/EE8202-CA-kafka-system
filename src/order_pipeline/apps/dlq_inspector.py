"""Dead letter queue inspector - the operator's view of what failed and why.

Run it with ``order-dlq`` (or ``python -m order_pipeline.apps.dlq_inspector``).

It reads the DLQ from the beginning in its own consumer group, so it never
interferes with the real consumer's offsets, and prints each entry with its
failure headers. Payloads that are still valid Avro are decoded; payloads that
are not (the reason many of them are in the DLQ) are shown as a hex preview.
"""

from __future__ import annotations

import argparse
import logging
import time
import uuid
from collections import Counter

from confluent_kafka import Consumer, KafkaError, Message

from order_pipeline.config import AppSettings, KafkaSettings
from order_pipeline.dlq import (
    HEADER_ATTEMPTS,
    HEADER_ERROR_MESSAGE,
    HEADER_REASON,
    normalize_headers,
)
from order_pipeline.logging_config import KAFKA_CLIENT_LOGGER, configure_logging
from order_pipeline.serdes import (
    MessageField,
    SerializationContext,
    build_schema_registry_client,
    raw_avro_deserializer,
)
from order_pipeline.shutdown import ShutdownSignal

logger = logging.getLogger("order_pipeline.dlq")

RULE = "-" * 78
HEX_PREVIEW_BYTES = 32


def _decode_headers(message: Message) -> dict[str, str]:
    """Render Kafka headers as printable strings."""
    return {
        key: value.decode("utf-8", errors="replace") for key, value in normalize_headers(message)
    }


def _describe_payload(message: Message, deserializer: object) -> str:
    """Decode the payload if possible, otherwise show a hex preview."""
    payload = message.value()
    if payload is None:
        return "<null>"

    ctx = SerializationContext(message.topic() or "", MessageField.VALUE)
    try:
        return repr(deserializer(payload, ctx))  # type: ignore[operator]
    except Exception:  # noqa: BLE001 - undecodable payloads are the point of the DLQ
        preview = payload[:HEX_PREVIEW_BYTES].hex(" ")
        suffix = " ..." if len(payload) > HEX_PREVIEW_BYTES else ""
        return f"<undecodable, {len(payload)} bytes> {preview}{suffix}"


def _print_entry(index: int, message: Message, payload: str, headers: dict[str, str]) -> None:
    key = (message.key() or b"").decode("utf-8", errors="replace")
    print(RULE)
    print(f"#{index}  {message.topic()}[{message.partition()}]@{message.offset()}  key={key!r}")
    print(f"  reason   : {headers.get(HEADER_REASON, '?')}")
    print(f"  attempts : {headers.get(HEADER_ATTEMPTS, '?')}")
    print(f"  error    : {headers.get(HEADER_ERROR_MESSAGE, '?')}")
    print(
        f"  origin   : {headers.get('x-origin-topic', '?')}"
        f"[{headers.get('x-origin-partition', '?')}]"
        f"@{headers.get('x-origin-offset', '?')}"
    )
    print(f"  payload  : {payload}")


def run(kafka: KafkaSettings, args: argparse.Namespace, shutdown: ShutdownSignal) -> Counter[str]:
    """Print DLQ entries until the topic is drained (or forever with --follow)."""
    group = args.group or f"dlq-inspector-{uuid.uuid4().hex[:8]}"
    consumer = Consumer(
        {
            "bootstrap.servers": kafka.bootstrap_servers,
            "group.id": group,
            "auto.offset.reset": "earliest",
            # Read-only tool: never move the real consumer's offsets.
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        },
        logger=logging.getLogger(KAFKA_CLIENT_LOGGER),
    )
    deserializer = raw_avro_deserializer(build_schema_registry_client(kafka))
    reasons: Counter[str] = Counter()
    seen = 0
    idle_since = time.monotonic()

    consumer.subscribe([kafka.dlq_topic])
    print(f"Reading '{kafka.dlq_topic}' from the beginning as group '{group}'...")

    try:
        while not shutdown.requested:
            message = consumer.poll(1.0)
            error = message.error() if message is not None else None

            # No message, or the end of a partition: the queue is drained.
            if message is None or (
                error is not None and error.code() == KafkaError._PARTITION_EOF  # noqa: SLF001
            ):
                if not args.follow and time.monotonic() - idle_since > args.idle_timeout:
                    break
                continue
            if error is not None:
                logger.warning("consumer error: %s", error)
                continue

            idle_since = time.monotonic()
            seen += 1
            headers = _decode_headers(message)
            reasons[headers.get(HEADER_REASON, "UNKNOWN")] += 1
            _print_entry(seen, message, _describe_payload(message, deserializer), headers)

            if args.limit and seen >= args.limit:
                break
    finally:
        consumer.close()

    print(RULE)
    if seen == 0:
        print("The dead letter queue is empty.")
    else:
        print(f"{seen} dead-lettered message(s):")
        for reason, count in reasons.most_common():
            print(f"  {count:5d}  {reason}")
    return reasons


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="order-dlq",
        description="Inspect the dead letter queue.",
    )
    parser.add_argument("--follow", action="store_true", help="keep waiting for new entries")
    parser.add_argument("--limit", type=int, default=0, help="stop after N entries (0 = all)")
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=3.0,
        help="seconds without new messages before exiting (ignored with --follow)",
    )
    parser.add_argument("--group", help="consumer group id (defaults to a throwaway group)")
    parser.add_argument("--log-level", help="DEBUG, INFO, WARNING, ...")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level or AppSettings().log_level)
    shutdown = ShutdownSignal().install()
    run(KafkaSettings(), args, shutdown)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
