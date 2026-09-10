"""Tests for the real-time running average."""

from __future__ import annotations

import random

import pytest

from order_pipeline.aggregation import OrderAggregator, RunningStats
from order_pipeline.models import Order


def order(product: str = "Item1", price: float = 10.0, order_id: str = "1") -> Order:
    return Order(order_id=order_id, product=product, price=price)


def test_a_fresh_aggregator_reports_nothing() -> None:
    aggregator = OrderAggregator()

    assert aggregator.count == 0
    assert aggregator.average_price == 0.0
    assert aggregator.snapshots() == [
        {
            "scope": "ALL",
            "count": 0,
            "averagePrice": 0.0,
            "sumPrice": 0.0,
            "minPrice": 0.0,
            "maxPrice": 0.0,
        }
    ]


def test_the_running_average_updates_after_every_order() -> None:
    aggregator = OrderAggregator()
    averages = []

    for price in (10.0, 20.0, 60.0):
        aggregator.add(order(price=price))
        averages.append(aggregator.average_price)

    assert averages == pytest.approx([10.0, 15.0, 30.0])


def test_min_max_and_total_track_the_stream() -> None:
    aggregator = OrderAggregator()

    for price in (5.0, 100.0, 45.0):
        aggregator.add(order(price=price))

    assert aggregator.overall.minimum == 5.0
    assert aggregator.overall.maximum == 100.0
    assert aggregator.overall.total == pytest.approx(150.0)


def test_per_product_averages_are_kept_separately() -> None:
    aggregator = OrderAggregator()
    aggregator.add(order(product="Item1", price=10.0))
    aggregator.add(order(product="Item1", price=30.0))
    aggregator.add(order(product="Item2", price=100.0))

    assert aggregator.per_product["Item1"].mean == pytest.approx(20.0)
    assert aggregator.per_product["Item2"].mean == pytest.approx(100.0)
    assert aggregator.average_price == pytest.approx(140.0 / 3)


def test_snapshots_lead_with_the_global_scope_then_sort_products() -> None:
    aggregator = OrderAggregator()
    aggregator.add(order(product="Item2"))
    aggregator.add(order(product="Item1"))

    assert [record["scope"] for record in aggregator.snapshots()] == ["ALL", "Item1", "Item2"]


def test_the_welford_mean_matches_a_plain_average_on_a_long_stream() -> None:
    rng = random.Random(42)
    prices = [rng.uniform(1, 1000) for _ in range(10_000)]
    aggregator = OrderAggregator()

    for price in prices:
        aggregator.add(order(price=price))

    assert aggregator.average_price == pytest.approx(sum(prices) / len(prices), rel=1e-9)


def test_the_welford_mean_stays_accurate_with_a_large_offset() -> None:
    """A naive sum loses precision here; the online mean does not."""
    stats = RunningStats()
    for value in (1e9, 1e9 + 1, 1e9 + 2):
        stats.update(value)

    assert stats.mean == pytest.approx(1e9 + 1, abs=1e-6)


def test_summary_line_reports_the_headline_average() -> None:
    aggregator = OrderAggregator()
    aggregator.add(order(product="Item1", price=10.0))
    aggregator.add(order(product="Item1", price=20.0))

    summary = aggregator.summary_line()

    assert "running avg=15.00" in summary
    assert "n=2" in summary
    assert "Item1=15.00" in summary
