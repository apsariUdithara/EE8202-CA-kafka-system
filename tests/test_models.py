"""Tests for the order model and its business rules."""

from __future__ import annotations

import math

import pytest

from order_pipeline.errors import ValidationError
from order_pipeline.models import Order, validate_order


def test_avro_round_trip_preserves_every_field() -> None:
    order = Order(order_id="1001", product="Item1", price=19.99)

    assert Order.from_avro(order.to_avro()) == order


def test_to_avro_uses_the_camel_case_schema_field_names() -> None:
    assert Order(order_id="1001", product="Item1", price=1.5).to_avro() == {
        "orderId": "1001",
        "product": "Item1",
        "price": 1.5,
    }


def test_from_avro_coerces_the_price_to_float() -> None:
    order = Order.from_avro({"orderId": "1001", "product": "Item1", "price": 25})

    assert isinstance(order.price, float)
    assert order.price == 25.0


@pytest.mark.parametrize("missing", ["orderId", "product", "price"])
def test_from_avro_rejects_a_record_with_a_missing_field(missing: str) -> None:
    record = {"orderId": "1001", "product": "Item1", "price": 10.0}
    del record[missing]

    with pytest.raises(ValidationError, match=missing):
        Order.from_avro(record)


def test_from_avro_rejects_a_price_that_is_not_numeric() -> None:
    with pytest.raises(ValidationError, match="wrong type"):
        Order.from_avro({"orderId": "1001", "product": "Item1", "price": "free"})


def test_orders_are_immutable() -> None:
    order = Order(order_id="1001", product="Item1", price=10.0)

    with pytest.raises(AttributeError):
        order.price = 20.0  # type: ignore[misc]


def test_validate_accepts_a_well_formed_order() -> None:
    validate_order(Order(order_id="1001", product="Item1", price=0.01))


@pytest.mark.parametrize(
    ("order", "expected"),
    [
        (Order(order_id="", product="Item1", price=10.0), "orderId"),
        (Order(order_id="   ", product="Item1", price=10.0), "orderId"),
        (Order(order_id="1001", product="", price=10.0), "product"),
        (Order(order_id="1001", product="Item1", price=0.0), "greater than zero"),
        (Order(order_id="1001", product="Item1", price=-5.0), "greater than zero"),
        (Order(order_id="1001", product="Item1", price=math.nan), "finite"),
        (Order(order_id="1001", product="Item1", price=math.inf), "finite"),
    ],
)
def test_validate_rejects_business_rule_breaches(order: Order, expected: str) -> None:
    with pytest.raises(ValidationError, match=expected):
        validate_order(order)
