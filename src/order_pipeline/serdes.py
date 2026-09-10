"""Avro serialisation wired to the Confluent Schema Registry.

Keeping the serialiser construction in one place means the producer, the
consumer and the DLQ inspector all register/resolve schemas identically, under
the standard ``<topic>-value`` subject naming strategy.
"""

from __future__ import annotations

from typing import cast

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringDeserializer,
    StringSerializer,
)

from order_pipeline.config import KafkaSettings
from order_pipeline.schemas import ORDER_AGGREGATE_SCHEMA, ORDER_SCHEMA, load_schema

__all__ = [
    "MessageField",
    "SerializationContext",
    "build_schema_registry_client",
    "key_deserializer",
    "key_serializer",
    "order_aggregate_serializer",
    "order_deserializer",
    "order_serializer",
    "raw_avro_deserializer",
]


def build_schema_registry_client(settings: KafkaSettings) -> SchemaRegistryClient:
    """Create a Schema Registry client from application settings."""
    return SchemaRegistryClient({"url": settings.schema_registry_url})


def key_serializer() -> StringSerializer:
    """Message keys are plain UTF-8 strings (the order id)."""
    return StringSerializer("utf_8")


def key_deserializer() -> StringDeserializer:
    """Decode the UTF-8 message key back into a string."""
    return StringDeserializer("utf_8")


def order_serializer(client: SchemaRegistryClient) -> AvroSerializer:
    """Serialise :class:`Order` objects using ``order.avsc``."""
    return cast(
        AvroSerializer,
        AvroSerializer(
            client,
            load_schema(ORDER_SCHEMA),
            to_dict=lambda order, _ctx: order.to_avro(),
            conf={"auto.register.schemas": True},
        ),
    )


def order_deserializer(client: SchemaRegistryClient) -> AvroDeserializer:
    """Decode order bytes into a plain dict.

    The dict is converted to an :class:`Order` by
    :meth:`Order.from_avro` so that a decode failure and a model-mapping
    failure stay distinguishable.
    """
    return cast(AvroDeserializer, AvroDeserializer(client, load_schema(ORDER_SCHEMA)))


def order_aggregate_serializer(client: SchemaRegistryClient) -> AvroSerializer:
    """Serialise running-average snapshots using ``order_aggregate.avsc``."""
    return cast(
        AvroSerializer,
        AvroSerializer(
            client,
            load_schema(ORDER_AGGREGATE_SCHEMA),
            to_dict=lambda record, _ctx: dict(record),
            conf={"auto.register.schemas": True},
        ),
    )


def raw_avro_deserializer(client: SchemaRegistryClient) -> AvroDeserializer:
    """A schema-less deserialiser that decodes whatever schema id it is given.

    Used by the DLQ inspector, which must read payloads whose schema it does
    not know in advance.
    """
    return cast(AvroDeserializer, AvroDeserializer(client))
