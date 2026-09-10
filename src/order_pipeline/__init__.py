"""EE8202 - Kafka + Avro order pipeline.

The package is deliberately split into two layers:

* a pure-Python domain core (:mod:`~order_pipeline.models`,
  :mod:`~order_pipeline.retry`, :mod:`~order_pipeline.aggregation`,
  :mod:`~order_pipeline.processing`) that has no Kafka dependency and is fully
  unit-testable without a broker; and
* thin Kafka adapters (:mod:`~order_pipeline.serdes`, :mod:`~order_pipeline.dlq`
  and the runnable apps under :mod:`order_pipeline.apps`).
"""

from __future__ import annotations

__version__ = "1.0.0"

__all__ = ["__version__"]
