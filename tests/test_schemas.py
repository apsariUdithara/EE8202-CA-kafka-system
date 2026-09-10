"""Guards on the Avro schemas.

The assignment fixes the order schema exactly; these tests fail loudly if a
future change drifts away from the brief.
"""

from __future__ import annotations

import json

import pytest
from fastavro import parse_schema

from order_pipeline.schemas import ORDER_AGGREGATE_SCHEMA, ORDER_SCHEMA, load_schema


@pytest.fixture(scope="module")
def order_schema() -> dict:
    return json.loads(load_schema(ORDER_SCHEMA))


def test_the_order_schema_matches_the_assignment_brief(order_schema: dict) -> None:
    fields = {field["name"]: field["type"] for field in order_schema["fields"]}

    assert order_schema["type"] == "record"
    assert order_schema["name"] == "Order"
    assert fields == {"orderId": "string", "product": "string", "price": "float"}


def test_the_order_schema_has_no_extra_fields(order_schema: dict) -> None:
    assert len(order_schema["fields"]) == 3


def test_every_schema_field_is_documented(order_schema: dict) -> None:
    assert all(field.get("doc") for field in order_schema["fields"])


@pytest.mark.parametrize("filename", [ORDER_SCHEMA, ORDER_AGGREGATE_SCHEMA])
def test_schemas_are_valid_avro(filename: str) -> None:
    parse_schema(json.loads(load_schema(filename)))


def test_a_missing_schema_file_fails_loudly() -> None:
    with pytest.raises(FileNotFoundError):
        load_schema("does_not_exist.avsc")
