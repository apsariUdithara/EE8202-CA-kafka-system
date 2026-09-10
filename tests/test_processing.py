"""Tests for the validate -> retry -> aggregate pipeline.

The whole failure matrix is exercised here with a fake sink and a fake clock,
so the suite runs in milliseconds and needs no broker.
"""

from __future__ import annotations

import random

import pytest

from order_pipeline.aggregation import OrderAggregator
from order_pipeline.errors import (
    PermanentProcessingError,
    RetryExhaustedError,
    TransientProcessingError,
    ValidationError,
)
from order_pipeline.models import Order
from order_pipeline.processing import FlakyDownstream, OrderProcessor
from order_pipeline.retry import RetryPolicy

ORDER = Order(order_id="1001", product="Item1", price=25.0)


class FakeClock:
    """Records the delays it was asked to sleep for, without sleeping."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


class ScriptedSink:
    """Raises the scripted exceptions in order, then accepts everything."""

    def __init__(self, *failures: Exception) -> None:
        self._failures = list(failures)
        self.calls = 0
        self.accepted: list[Order] = []

    def __call__(self, order: Order) -> None:
        self.calls += 1
        if self._failures:
            raise self._failures.pop(0)
        self.accepted.append(order)


def build(
    sink: ScriptedSink, clock: FakeClock, **policy: float
) -> tuple[OrderProcessor, OrderAggregator]:
    aggregator = OrderAggregator()
    processor = OrderProcessor(
        aggregator=aggregator,
        sink=sink,
        retry_policy=RetryPolicy(jitter_ratio=0, initial_backoff_s=0.1, **policy),  # type: ignore[arg-type]
        sleep=clock,
        rng=random.Random(0),
    )
    return processor, aggregator


def test_a_healthy_order_is_delivered_once_and_aggregated() -> None:
    sink, clock = ScriptedSink(), FakeClock()
    processor, aggregator = build(sink, clock)

    outcome = processor.handle(ORDER)

    assert outcome.attempts == 1
    assert not outcome.was_retried
    assert sink.accepted == [ORDER]
    assert clock.delays == []
    assert aggregator.count == 1


def test_a_transient_failure_is_retried_until_it_succeeds() -> None:
    sink = ScriptedSink(
        TransientProcessingError("downstream 503"),
        TransientProcessingError("downstream 503"),
    )
    clock = FakeClock()
    processor, aggregator = build(sink, clock, max_attempts=4)

    outcome = processor.handle(ORDER)

    assert outcome.attempts == 3
    assert outcome.was_retried
    assert sink.calls == 3
    assert clock.delays == pytest.approx([0.1, 0.2])
    assert aggregator.count == 1


def test_retries_stop_at_the_budget_and_raise_retry_exhausted() -> None:
    sink = ScriptedSink(*[TransientProcessingError("always down") for _ in range(10)])
    clock = FakeClock()
    processor, aggregator = build(sink, clock, max_attempts=3)

    with pytest.raises(RetryExhaustedError) as excinfo:
        processor.handle(ORDER)

    assert excinfo.value.attempts == 3
    assert sink.calls == 3
    # Backoff is applied between attempts only, never after the final one.
    assert len(clock.delays) == 2
    assert aggregator.count == 0


def test_an_invalid_order_is_rejected_before_the_sink_is_touched() -> None:
    sink, clock = ScriptedSink(), FakeClock()
    processor, aggregator = build(sink, clock)

    with pytest.raises(ValidationError):
        processor.handle(Order(order_id="1002", product="Item1", price=-1.0))

    assert sink.calls == 0
    assert aggregator.count == 0


def test_a_permanent_sink_failure_is_not_retried() -> None:
    sink = ScriptedSink(PermanentProcessingError("unknown product code"))
    clock = FakeClock()
    processor, aggregator = build(sink, clock, max_attempts=5)

    with pytest.raises(PermanentProcessingError):
        processor.handle(ORDER)

    assert sink.calls == 1
    assert clock.delays == []
    assert aggregator.count == 0


def test_a_failed_order_is_never_counted_in_the_running_average() -> None:
    sink = ScriptedSink(*[TransientProcessingError("down") for _ in range(10)])
    clock = FakeClock()
    processor, aggregator = build(sink, clock, max_attempts=2)

    with pytest.raises(RetryExhaustedError):
        processor.handle(ORDER)
    sink._failures.clear()  # noqa: SLF001 - the downstream has "recovered"
    processor.handle(Order(order_id="1003", product="Item2", price=50.0))

    assert aggregator.count == 1
    assert aggregator.average_price == pytest.approx(50.0)


def test_a_single_attempt_policy_dead_letters_on_the_first_failure() -> None:
    sink = ScriptedSink(TransientProcessingError("down"))
    clock = FakeClock()
    processor, _ = build(sink, clock, max_attempts=1)

    with pytest.raises(RetryExhaustedError):
        processor.handle(ORDER)

    assert sink.calls == 1
    assert clock.delays == []


class TestFlakyDownstream:
    def test_it_never_fails_when_the_rate_is_zero(self) -> None:
        sink = FlakyDownstream(failure_rate=0.0, rng=random.Random(1))

        for _ in range(50):
            sink(ORDER)

        assert sink.delivered == 50
        assert sink.failures == 0

    def test_it_always_fails_when_the_rate_is_one(self) -> None:
        sink = FlakyDownstream(failure_rate=1.0, rng=random.Random(1))

        with pytest.raises(TransientProcessingError):
            sink(ORDER)

    def test_the_failure_rate_must_be_a_probability(self) -> None:
        with pytest.raises(ValueError, match=r"\[0, 1\]"):
            FlakyDownstream(failure_rate=2.0)
