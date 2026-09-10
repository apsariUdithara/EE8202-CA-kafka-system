"""The order domain model and its mapping to/from the Avro record."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from order_pipeline.errors import ValidationError

#: Field names exactly as they appear in ``schemas/order.avsc``.
AVRO_FIELDS = ("orderId", "product", "price")


@dataclass(frozen=True, slots=True)
class Order:
    """A single purchase transaction.

    Attribute names are snake_case (Python convention) while the wire format
    keeps the camelCase names mandated by the assignment schema; the two
    ``*_avro`` helpers are the only place that mapping lives.
    """

    order_id: str
    product: str
    price: float

    @classmethod
    def from_avro(cls, record: dict[str, Any]) -> Order:
        """Build an order from a decoded Avro record.

        Raises:
            ValidationError: if a schema field is missing or has the wrong type.
        """
        missing = [name for name in AVRO_FIELDS if name not in record]
        if missing:
            raise ValidationError(f"record is missing required field(s): {', '.join(missing)}")

        try:
            return cls(
                order_id=str(record["orderId"]),
                product=str(record["product"]),
                price=float(record["price"]),
            )
        except (TypeError, ValueError) as exc:
            raise ValidationError(f"record has a field of the wrong type: {exc}") from exc

    def to_avro(self) -> dict[str, Any]:
        """Render the order as a dict matching ``schemas/order.avsc``."""
        return {"orderId": self.order_id, "product": self.product, "price": self.price}


def validate_order(order: Order) -> None:
    """Enforce the business rules an order must satisfy to be aggregated.

    These are deliberately checks the Avro schema *cannot* express - the schema
    guarantees a float is present, not that it is a sane price.

    Raises:
        ValidationError: on any rule breach. It is a permanent error, so the
            consumer dead-letters the message instead of retrying it.
    """
    if not order.order_id.strip():
        raise ValidationError("orderId must not be blank")
    if not order.product.strip():
        raise ValidationError("product must not be blank")
    if math.isnan(order.price) or math.isinf(order.price):
        raise ValidationError(f"price must be a finite number, got {order.price!r}")
    if order.price <= 0:
        raise ValidationError(f"price must be greater than zero, got {order.price!r}")
