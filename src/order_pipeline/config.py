"""Typed configuration, resolved from environment variables and an optional .env.

Every setting has a default that works against the bundled docker-compose
stack, so a fresh clone runs with no configuration at all. Nothing in the code
base reads ``os.environ`` directly.
"""

from __future__ import annotations

from pydantic import Field, ValidationInfo, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


class KafkaSettings(BaseSettings):
    """Broker, Schema Registry and topic names."""

    model_config = _ENV_FILE | SettingsConfigDict(env_prefix="KAFKA_")

    bootstrap_servers: str = "localhost:29092"
    schema_registry_url: str = "http://localhost:8081"
    orders_topic: str = "orders"
    dlq_topic: str = "orders.DLQ"
    aggregates_topic: str = "orders.aggregates"


class ProducerSettings(BaseSettings):
    """Traffic shape of the synthetic order stream."""

    model_config = _ENV_FILE | SettingsConfigDict(env_prefix="PRODUCER_")

    message_rate_per_second: float = Field(default=2.0, gt=0)
    max_messages: int = Field(default=0, ge=0, description="0 runs until interrupted")
    invalid_payload_rate: float = Field(default=0.05, ge=0, le=1)
    corrupt_payload_rate: float = Field(default=0.03, ge=0, le=1)
    products: list[str] = Field(default_factory=lambda: [f"Item{n}" for n in range(1, 6)])
    min_price: float = Field(default=5.0, gt=0)
    max_price: float = Field(default=500.0, gt=0)
    seed: int | None = None

    @field_validator("products")
    @classmethod
    def _require_products(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("at least one product name is required")
        return value

    @field_validator("max_price")
    @classmethod
    def _check_price_range(cls, value: float, info: ValidationInfo) -> float:
        minimum = info.data.get("min_price")
        if minimum is not None and value < minimum:
            raise ValueError("max_price must not be smaller than min_price")
        return value

    @property
    def failure_injection_rate(self) -> float:
        return self.invalid_payload_rate + self.corrupt_payload_rate


class ConsumerSettings(BaseSettings):
    """Consumer group, retry budget and aggregate publication cadence."""

    model_config = _ENV_FILE | SettingsConfigDict(env_prefix="CONSUMER_")

    group_id: str = "order-consumer"
    auto_offset_reset: str = "earliest"

    max_retry_attempts: int = Field(default=4, ge=1)
    retry_initial_backoff_s: float = Field(default=0.2, ge=0)
    retry_backoff_multiplier: float = Field(default=2.0, ge=1)
    retry_max_backoff_s: float = Field(default=5.0, ge=0)
    retry_jitter_ratio: float = Field(default=0.2, ge=0, le=1)

    transient_failure_rate: float = Field(default=0.15, ge=0, le=1)

    publish_aggregates: bool = True
    aggregate_emit_every_n: int = Field(default=10, ge=1)
    aggregate_emit_interval_s: float = Field(default=5.0, gt=0)


class AppSettings(BaseSettings):
    """Cross-cutting settings."""

    model_config = _ENV_FILE

    log_level: str = "INFO"
