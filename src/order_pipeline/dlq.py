"""Dead letter queue publication.

Design decisions worth stating explicitly:

* The **original value bytes are forwarded unchanged**. Re-serialising would
  be impossible for the very case the DLQ exists to catch - a payload that
  could not be decoded in the first place - and would destroy the evidence
  needed to diagnose it.
* The failure context travels in **Kafka headers**, not inside the payload, so
  the DLQ topic stays byte-compatible with the source topic and a fixed
  message can be replayed by copying its value straight back to ``orders``.
"""

from __future__ import annotations

import logging
import socket
import time
from enum import StrEnum

from confluent_kafka import KafkaError, Message, Producer

from order_pipeline.errors import (
    DeserializationError,
    PermanentProcessingError,
    RetryExhaustedError,
)

logger = logging.getLogger(__name__)

HEADER_REASON = "x-dlq-reason"
HEADER_ERROR_CLASS = "x-dlq-error-class"
HEADER_ERROR_MESSAGE = "x-dlq-error-message"
HEADER_ATTEMPTS = "x-dlq-attempts"
HEADER_FAILED_AT = "x-dlq-failed-at-ms"
HEADER_CONSUMER_GROUP = "x-dlq-consumer-group"
HEADER_HOST = "x-dlq-host"
HEADER_ORIGIN_TOPIC = "x-origin-topic"
HEADER_ORIGIN_PARTITION = "x-origin-partition"
HEADER_ORIGIN_OFFSET = "x-origin-offset"

#: Header keys this consumer owns; stripped before new ones are written so a
#: replayed message cannot accumulate contradictory failure context.
_MANAGED_HEADER_PREFIXES = ("x-dlq-", "x-origin-")


def normalize_headers(message: Message) -> list[tuple[str, bytes]]:
    """Return a message's headers as ``(key, bytes)`` pairs.

    librdkafka hands headers back as a list of pairs, a dict, or ``None``, and
    a value may be ``str``, ``bytes`` or ``None``. Normalising once here keeps
    that noise out of every caller.
    """
    raw = message.headers()
    if raw is None:
        return []
    items = raw.items() if isinstance(raw, dict) else raw
    normalized: list[tuple[str, bytes]] = []
    for key, value in items:
        if isinstance(value, str):
            normalized.append((key, value.encode()))
        else:
            normalized.append((key, value or b""))
    return normalized


class DeadLetterReason(StrEnum):
    """Why a message was dead-lettered - the first thing to look at in the DLQ."""

    DESERIALIZATION_FAILURE = "DESERIALIZATION_FAILURE"
    VALIDATION_FAILURE = "VALIDATION_FAILURE"
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"
    UNEXPECTED_ERROR = "UNEXPECTED_ERROR"


def classify(error: BaseException) -> DeadLetterReason:
    """Map an exception onto the reason recorded in the DLQ headers."""
    if isinstance(error, DeserializationError):
        return DeadLetterReason.DESERIALIZATION_FAILURE
    if isinstance(error, RetryExhaustedError):
        return DeadLetterReason.RETRY_EXHAUSTED
    if isinstance(error, PermanentProcessingError):
        return DeadLetterReason.VALIDATION_FAILURE
    return DeadLetterReason.UNEXPECTED_ERROR


class DeadLetterPublisher:
    """Forwards failed messages, verbatim, to the dead letter topic."""

    def __init__(self, producer: Producer, topic: str, consumer_group: str) -> None:
        self._producer = producer
        self._topic = topic
        self._consumer_group = consumer_group
        self._host = socket.gethostname()
        self.published = 0

    def publish(self, message: Message, error: BaseException, attempts: int = 1) -> None:
        """Copy ``message`` to the DLQ, annotated with the failure context."""
        reason = classify(error)
        headers = self._build_headers(message, error, reason, attempts)

        self._producer.produce(
            topic=self._topic,
            key=message.key(),
            value=message.value(),
            headers=list(headers),
            on_delivery=self._on_delivery,
        )
        # Keep the DLQ ahead of the offset commit: a message must be safely in
        # the dead letter topic before the source offset is allowed to advance.
        self._producer.flush(timeout=10.0)
        self.published += 1

        logger.error(
            "dead-lettered %s[%d]@%d after %d attempt(s) - %s: %s",
            message.topic(),
            message.partition(),
            message.offset(),
            attempts,
            reason.value,
            error,
        )

    def _build_headers(
        self,
        message: Message,
        error: BaseException,
        reason: DeadLetterReason,
        attempts: int,
    ) -> list[tuple[str, bytes]]:
        # Drop any DLQ headers from a previous round so re-processing a
        # replayed message cannot accumulate stale context.
        preserved = [
            (key, value)
            for key, value in normalize_headers(message)
            if not key.startswith(_MANAGED_HEADER_PREFIXES)
        ]
        return [
            *preserved,
            (HEADER_REASON, reason.value.encode()),
            (HEADER_ERROR_CLASS, type(error).__name__.encode()),
            (HEADER_ERROR_MESSAGE, str(error)[:1024].encode("utf-8", errors="replace")),
            (HEADER_ATTEMPTS, str(attempts).encode()),
            (HEADER_FAILED_AT, str(int(time.time() * 1000)).encode()),
            (HEADER_CONSUMER_GROUP, self._consumer_group.encode()),
            (HEADER_HOST, self._host.encode()),
            (HEADER_ORIGIN_TOPIC, (message.topic() or "").encode()),
            (HEADER_ORIGIN_PARTITION, str(message.partition()).encode()),
            (HEADER_ORIGIN_OFFSET, str(message.offset()).encode()),
        ]

    @staticmethod
    def _on_delivery(err: KafkaError | None, msg: Message) -> None:
        if err is not None:  # pragma: no cover - broker-side failure
            logger.critical("FAILED to write to the dead letter queue: %s", err)
        else:
            logger.debug("dlq delivery ok: %s[%d]@%d", msg.topic(), msg.partition(), msg.offset())
