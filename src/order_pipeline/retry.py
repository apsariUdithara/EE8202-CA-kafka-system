"""Exponential backoff policy used when a transient failure is retried."""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Truncated exponential backoff with full-ratio jitter.

    Jitter matters even for a single consumer: without it, a downstream that
    recovers on a timer receives every retry of every in-flight message in the
    same instant and is knocked over again (the classic thundering herd).

    Attributes:
        max_attempts: Total tries, including the first. ``1`` disables retrying.
        initial_backoff_s: Delay before the second attempt.
        multiplier: Growth factor applied per attempt.
        max_backoff_s: Ceiling applied before jitter.
        jitter_ratio: Fraction of the delay randomised, in ``[0, 1]``.
    """

    max_attempts: int = 4
    initial_backoff_s: float = 0.2
    multiplier: float = 2.0
    max_backoff_s: float = 5.0
    jitter_ratio: float = 0.2

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.initial_backoff_s < 0:
            raise ValueError("initial_backoff_s must not be negative")
        if self.multiplier < 1:
            raise ValueError("multiplier must be at least 1")
        if self.max_backoff_s < self.initial_backoff_s:
            raise ValueError("max_backoff_s must not be smaller than initial_backoff_s")
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("jitter_ratio must be within [0, 1]")

    def should_retry(self, attempt: int) -> bool:
        """Whether an ``attempt`` (1-based) that just failed may be retried."""
        return attempt < self.max_attempts

    def backoff_for(self, attempt: int, rng: random.Random | None = None) -> float:
        """Seconds to wait after a failed 1-based ``attempt``.

        The returned delay lies in
        ``[base * (1 - jitter_ratio), base * (1 + jitter_ratio)]`` and is never
        negative.
        """
        if attempt < 1:
            raise ValueError("attempt is 1-based and must be at least 1")

        base = min(self.initial_backoff_s * self.multiplier ** (attempt - 1), self.max_backoff_s)
        if self.jitter_ratio == 0:
            return base

        source = rng or random
        spread = base * self.jitter_ratio
        return max(0.0, base + source.uniform(-spread, spread))
