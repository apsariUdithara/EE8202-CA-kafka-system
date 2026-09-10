"""Real-time aggregation: a running average of order prices.

The average is maintained with Welford's online algorithm rather than the
obvious ``total / count``. Both are O(1) per message, but Welford keeps the
mean numerically stable over a long-running stream, where a naive accumulator
loses precision once the total grows far larger than an individual price.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from order_pipeline.models import Order


@dataclass(slots=True)
class RunningStats:
    """Streaming count / mean / min / max for one scope."""

    count: int = 0
    mean: float = 0.0
    minimum: float = math.inf
    maximum: float = -math.inf

    def update(self, value: float) -> None:
        """Fold one more observation into the statistics."""
        self.count += 1
        # Welford: mean_n = mean_(n-1) + (x - mean_(n-1)) / n
        self.mean += (value - self.mean) / self.count
        self.minimum = min(self.minimum, value)
        self.maximum = max(self.maximum, value)

    @property
    def total(self) -> float:
        """Sum of every observation, derived from the running mean."""
        return self.mean * self.count

    def as_dict(self, scope: str) -> dict[str, float | int | str]:
        """Render the statistics for logging or Avro publication."""
        empty = self.count == 0
        return {
            "scope": scope,
            "count": self.count,
            "averagePrice": self.mean,
            "sumPrice": self.total,
            "minPrice": 0.0 if empty else self.minimum,
            "maxPrice": 0.0 if empty else self.maximum,
        }


@dataclass(slots=True)
class OrderAggregator:
    """Keeps the global running average plus a per-product breakdown.

    Only orders that were *successfully* processed are folded in, so the
    aggregate never counts a message that ended up in the dead letter queue.
    """

    overall: RunningStats = field(default_factory=RunningStats)
    per_product: dict[str, RunningStats] = field(default_factory=dict)

    def add(self, order: Order) -> None:
        """Include one processed order in the running statistics."""
        self.overall.update(order.price)
        self.per_product.setdefault(order.product, RunningStats()).update(order.price)

    @property
    def count(self) -> int:
        """Number of orders aggregated so far."""
        return self.overall.count

    @property
    def average_price(self) -> float:
        """The headline figure: running average across every product."""
        return self.overall.mean

    def snapshots(self) -> list[dict[str, float | int | str]]:
        """One record for the global scope, then one per product (A-Z)."""
        records = [self.overall.as_dict("ALL")]
        records.extend(
            stats.as_dict(product) for product, stats in sorted(self.per_product.items())
        )
        return records

    def summary_line(self) -> str:
        """A one-line, human-readable form for the live demonstration."""
        products = ", ".join(
            f"{product}={stats.mean:.2f}(n={stats.count})"
            for product, stats in sorted(self.per_product.items())
        )
        return (
            f"running avg={self.overall.mean:.2f} over n={self.overall.count} "
            f"[min={self.overall.minimum:.2f} max={self.overall.maximum:.2f}]"
            + (f" | {products}" if products else "")
        )
