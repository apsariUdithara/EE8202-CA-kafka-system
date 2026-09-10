"""Tests for the exponential backoff retry policy."""

from __future__ import annotations

import random

import pytest

from order_pipeline.retry import RetryPolicy


def test_backoff_grows_exponentially_when_jitter_is_disabled() -> None:
    policy = RetryPolicy(initial_backoff_s=0.1, multiplier=2.0, max_backoff_s=100, jitter_ratio=0)

    delays = [policy.backoff_for(attempt) for attempt in (1, 2, 3, 4)]

    assert delays == pytest.approx([0.1, 0.2, 0.4, 0.8])


def test_backoff_is_capped_at_max_backoff() -> None:
    policy = RetryPolicy(
        max_attempts=10, initial_backoff_s=1.0, multiplier=10.0, max_backoff_s=5.0, jitter_ratio=0
    )

    assert policy.backoff_for(5) == 5.0


def test_jitter_stays_within_the_configured_band() -> None:
    policy = RetryPolicy(
        initial_backoff_s=1.0, multiplier=1.0, max_backoff_s=1.0, jitter_ratio=0.25
    )
    rng = random.Random(1234)

    delays = [policy.backoff_for(1, rng) for _ in range(500)]

    assert all(0.75 <= delay <= 1.25 for delay in delays)
    # Jitter must actually vary, otherwise it is not spreading anything out.
    assert len(set(delays)) > 1


def test_jitter_never_produces_a_negative_delay() -> None:
    policy = RetryPolicy(initial_backoff_s=0.01, multiplier=1.0, max_backoff_s=0.01, jitter_ratio=1)
    rng = random.Random(7)

    assert all(policy.backoff_for(1, rng) >= 0 for _ in range(200))


def test_should_retry_respects_the_attempt_budget() -> None:
    policy = RetryPolicy(max_attempts=3)

    assert policy.should_retry(1)
    assert policy.should_retry(2)
    assert not policy.should_retry(3)


def test_a_single_attempt_policy_disables_retrying() -> None:
    assert not RetryPolicy(max_attempts=1).should_retry(1)


def test_backoff_rejects_a_zero_or_negative_attempt() -> None:
    with pytest.raises(ValueError, match="1-based"):
        RetryPolicy().backoff_for(0)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"initial_backoff_s": -1},
        {"multiplier": 0.5},
        {"initial_backoff_s": 10, "max_backoff_s": 1},
        {"jitter_ratio": 1.5},
    ],
)
def test_invalid_policies_are_rejected_at_construction(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)  # type: ignore[arg-type]
