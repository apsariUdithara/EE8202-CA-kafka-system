"""Tests for dead letter routing and the headers written alongside a failure."""

from __future__ import annotations

from typing import Any

import pytest

from order_pipeline.dlq import (
    HEADER_ATTEMPTS,
    HEADER_ERROR_CLASS,
    HEADER_ERROR_MESSAGE,
    HEADER_ORIGIN_OFFSET,
    HEADER_ORIGIN_TOPIC,
    HEADER_REASON,
    DeadLetterPublisher,
    DeadLetterReason,
    classify,
)
from order_pipeline.errors import (
    DeserializationError,
    RetryExhaustedError,
    TransientProcessingError,
    ValidationError,
)


class FakeMessage:
    """The subset of confluent_kafka.Message the publisher actually uses."""

    def __init__(
        self,
        value: bytes | None = b"\x00payload",
        key: bytes | None = b"1001",
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        self._value = value
        self._key = key
        self._headers = headers

    def value(self) -> bytes | None:
        return self._value

    def key(self) -> bytes | None:
        return self._key

    def headers(self) -> list[tuple[str, bytes]] | None:
        return self._headers

    def topic(self) -> str:
        return "orders"

    def partition(self) -> int:
        return 2

    def offset(self) -> int:
        return 77


class FakeProducer:
    """Captures produced records instead of talking to a broker."""

    def __init__(self) -> None:
        self.produced: list[dict[str, Any]] = []
        self.flushes = 0

    def produce(self, **kwargs: Any) -> None:
        self.produced.append(kwargs)

    def flush(self, timeout: float = 0.0) -> int:
        self.flushes += 1
        return 0


@pytest.fixture
def publisher() -> tuple[DeadLetterPublisher, FakeProducer]:
    producer = FakeProducer()
    return DeadLetterPublisher(producer, "orders.DLQ", "order-consumer"), producer  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (DeserializationError("bad bytes"), DeadLetterReason.DESERIALIZATION_FAILURE),
        (ValidationError("negative price"), DeadLetterReason.VALIDATION_FAILURE),
        (
            RetryExhaustedError(3, TransientProcessingError("down")),
            DeadLetterReason.RETRY_EXHAUSTED,
        ),
        (RuntimeError("something else"), DeadLetterReason.UNEXPECTED_ERROR),
    ],
)
def test_errors_are_classified_into_dlq_reasons(error: Exception, expected: str) -> None:
    assert classify(error) == expected


def test_the_original_bytes_and_key_are_forwarded_unchanged(
    publisher: tuple[DeadLetterPublisher, FakeProducer],
) -> None:
    dlq, producer = publisher
    message = FakeMessage(value=b"\x00\x00not-avro", key=b"1001")

    dlq.publish(message, DeserializationError("bad bytes"))  # type: ignore[arg-type]

    record = producer.produced[0]
    assert record["topic"] == "orders.DLQ"
    assert record["value"] == b"\x00\x00not-avro"
    assert record["key"] == b"1001"


def test_the_failure_context_is_written_to_headers(
    publisher: tuple[DeadLetterPublisher, FakeProducer],
) -> None:
    dlq, producer = publisher
    error = RetryExhaustedError(4, TransientProcessingError("downstream 503"))

    dlq.publish(FakeMessage(), error, attempts=4)  # type: ignore[arg-type]

    headers = dict(producer.produced[0]["headers"])
    assert headers[HEADER_REASON] == b"RETRY_EXHAUSTED"
    assert headers[HEADER_ERROR_CLASS] == b"RetryExhaustedError"
    assert b"downstream 503" in headers[HEADER_ERROR_MESSAGE]
    assert headers[HEADER_ATTEMPTS] == b"4"
    assert headers[HEADER_ORIGIN_TOPIC] == b"orders"
    assert headers[HEADER_ORIGIN_OFFSET] == b"77"


def test_the_dlq_write_is_flushed_before_the_offset_can_advance(
    publisher: tuple[DeadLetterPublisher, FakeProducer],
) -> None:
    dlq, producer = publisher

    dlq.publish(FakeMessage(), ValidationError("blank orderId"))  # type: ignore[arg-type]

    assert producer.flushes == 1
    assert dlq.published == 1


def test_application_headers_survive_but_stale_dlq_headers_are_replaced(
    publisher: tuple[DeadLetterPublisher, FakeProducer],
) -> None:
    dlq, producer = publisher
    message = FakeMessage(
        headers=[("trace-id", b"abc-123"), (HEADER_REASON, b"VALIDATION_FAILURE")]
    )

    dlq.publish(message, DeserializationError("bad bytes"))  # type: ignore[arg-type]

    headers = producer.produced[0]["headers"]
    assert ("trace-id", b"abc-123") in headers
    assert [value for key, value in headers if key == HEADER_REASON] == [b"DESERIALIZATION_FAILURE"]


def test_a_very_long_error_message_is_truncated(
    publisher: tuple[DeadLetterPublisher, FakeProducer],
) -> None:
    dlq, producer = publisher

    dlq.publish(FakeMessage(), ValidationError("x" * 5000))  # type: ignore[arg-type]

    headers = dict(producer.produced[0]["headers"])
    assert len(headers[HEADER_ERROR_MESSAGE]) <= 1024
