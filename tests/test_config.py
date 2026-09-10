"""Tests for the typed settings layer.

Every test passes ``_env_file=None`` so a developer's local ``.env`` cannot
change the result.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError as PydanticValidationError

from order_pipeline.config import ConsumerSettings, KafkaSettings, ProducerSettings


def test_defaults_target_the_bundled_compose_stack() -> None:
    settings = KafkaSettings(_env_file=None)

    assert settings.bootstrap_servers == "localhost:29092"
    assert settings.schema_registry_url == "http://localhost:8081"
    assert (settings.orders_topic, settings.dlq_topic) == ("orders", "orders.DLQ")


def test_kafka_settings_read_the_prefixed_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker-1:9092,broker-2:9092")
    monkeypatch.setenv("KAFKA_ORDERS_TOPIC", "prod.orders")

    settings = KafkaSettings(_env_file=None)

    assert settings.bootstrap_servers == "broker-1:9092,broker-2:9092"
    assert settings.orders_topic == "prod.orders"


def test_consumer_settings_read_the_prefixed_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CONSUMER_MAX_RETRY_ATTEMPTS", "9")
    monkeypatch.setenv("CONSUMER_PUBLISH_AGGREGATES", "false")

    settings = ConsumerSettings(_env_file=None)

    assert settings.max_retry_attempts == 9
    assert settings.publish_aggregates is False


def test_the_producer_generates_five_items_by_default() -> None:
    assert ProducerSettings(_env_file=None).products == [
        "Item1",
        "Item2",
        "Item3",
        "Item4",
        "Item5",
    ]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"message_rate_per_second": 0},
        {"max_messages": -1},
        {"invalid_payload_rate": 1.5},
        {"corrupt_payload_rate": -0.1},
        {"products": []},
        {"min_price": 100.0, "max_price": 1.0},
    ],
)
def test_nonsensical_producer_settings_are_rejected(kwargs: dict[str, object]) -> None:
    with pytest.raises(PydanticValidationError):
        ProducerSettings(_env_file=None, **kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_retry_attempts": 0},
        {"retry_backoff_multiplier": 0.5},
        {"retry_jitter_ratio": 2.0},
        {"transient_failure_rate": -1},
        {"aggregate_emit_every_n": 0},
        {"aggregate_emit_interval_s": 0},
    ],
)
def test_nonsensical_consumer_settings_are_rejected(kwargs: dict[str, object]) -> None:
    with pytest.raises(PydanticValidationError):
        ConsumerSettings(_env_file=None, **kwargs)  # type: ignore[arg-type]


def test_a_single_retry_attempt_is_allowed() -> None:
    """max_attempts=1 is the documented way to switch retrying off."""
    assert ConsumerSettings(_env_file=None, max_retry_attempts=1).max_retry_attempts == 1


def test_failure_injection_rates_are_summed_for_reporting() -> None:
    settings = ProducerSettings(_env_file=None, invalid_payload_rate=0.1, corrupt_payload_rate=0.2)

    assert settings.failure_injection_rate == pytest.approx(0.3)
