"""The order-handling pipeline: validate -> deliver (with retries) -> aggregate.

This module owns the retry decision and nothing else knows about it. It is
free of Kafka imports on purpose, so the whole failure matrix can be exercised
by fast unit tests with a fake sink and a fake clock.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from order_pipeline.aggregation import OrderAggregator
from order_pipeline.errors import (
    PermanentProcessingError,
    RetryExhaustedError,
    TransientProcessingError,
)
from order_pipeline.models import Order, validate_order
from order_pipeline.retry import RetryPolicy

logger = logging.getLogger(__name__)


class OrderSink(Protocol):
    """The downstream an order is delivered to once it has been validated.

    In a production system this would be a warehouse API, a database, or a
    payment gateway. It is a protocol so the consumer never depends on which.
    """

    def __call__(self, order: Order) -> None:  # pragma: no cover - interface only
        ...


@dataclass(frozen=True, slots=True)
class ProcessingOutcome:
    """What happened to one order."""

    order: Order
    attempts: int

    @property
    def was_retried(self) -> bool:
        return self.attempts > 1


class FlakyDownstream:
    """A sink that fails a configurable share of deliveries, then succeeds.

    Real transient faults cannot be summoned on demand during a live
    demonstration, so this stands in for one: it raises
    :class:`TransientProcessingError` with probability ``failure_rate`` on every
    attempt, which makes the retry path - and eventually the dead letter queue,
    when several attempts in a row lose the coin flip - genuinely observable.
    """

    def __init__(self, failure_rate: float = 0.15, rng: random.Random | None = None) -> None:
        if not 0 <= failure_rate <= 1:
            raise ValueError("failure_rate must be within [0, 1]")
        self.failure_rate = failure_rate
        self._rng = rng or random.Random()
        self.delivered = 0
        self.failures = 0

    def __call__(self, order: Order) -> None:
        if self._rng.random() < self.failure_rate:
            self.failures += 1
            raise TransientProcessingError(
                f"simulated downstream outage while delivering order {order.order_id}"
            )
        self.delivered += 1


class OrderProcessor:
    """Applies validation, bounded retries and aggregation to each order."""

    def __init__(
        self,
        aggregator: OrderAggregator,
        sink: OrderSink,
        retry_policy: RetryPolicy | None = None,
        *,
        sleep: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self._aggregator = aggregator
        self._sink = sink
        self._policy = retry_policy or RetryPolicy()
        self._sleep = sleep
        self._rng = rng or random.Random()

    def handle(self, order: Order) -> ProcessingOutcome:
        """Process one order.

        Returns:
            The outcome, including how many attempts delivery took.

        Raises:
            PermanentProcessingError: the order is invalid, or the sink reported
                a failure that retrying cannot fix. Dead-letter it immediately.
            RetryExhaustedError: the sink kept failing transiently until the
                retry budget ran out. Dead-letter it.
        """
        validate_order(order)

        for attempt in range(1, self._policy.max_attempts + 1):
            try:
                self._sink(order)
            except PermanentProcessingError:
                # Nothing to gain from another attempt - let it bubble up.
                raise
            except TransientProcessingError as exc:
                if not self._policy.should_retry(attempt):
                    logger.warning(
                        "order %s exhausted its retry budget after %d attempt(s): %s",
                        order.order_id,
                        attempt,
                        exc,
                    )
                    raise RetryExhaustedError(attempt, exc) from exc

                delay = self._policy.backoff_for(attempt, self._rng)
                logger.info(
                    "order %s failed transiently on attempt %d/%d (%s); retrying in %.2fs",
                    order.order_id,
                    attempt,
                    self._policy.max_attempts,
                    exc,
                    delay,
                )
                self._sleep(delay)
            else:
                self._aggregator.add(order)
                return ProcessingOutcome(order=order, attempts=attempt)

        # Unreachable: the loop either returns or raises.
        raise AssertionError("retry loop terminated without a result")  # pragma: no cover
