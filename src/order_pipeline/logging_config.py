"""Console logging shared by every entry point."""

from __future__ import annotations

import logging
import sys

_FORMAT = "%(asctime)s %(levelname)-7s %(name)-28s %(message)s"
_DATEFMT = "%H:%M:%S"

#: Every Kafka client is constructed with this logger, so librdkafka's own
#: messages arrive through Python logging instead of being written straight to
#: stderr in its native ``%4|1699…|GETPID|…`` format.
KAFKA_CLIENT_LOGGER = "order_pipeline.kafka"


def configure_logging(level: str = "INFO") -> None:
    """Install a single stdout handler with a demo-friendly format.

    Idempotent: calling it twice does not duplicate log lines.
    """
    root = logging.getLogger()
    root.setLevel(level.upper())

    for handler in root.handlers[:]:
        root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATEFMT))
    root.addHandler(handler)

    # Third-party chatter that would otherwise bury the pipeline's own output:
    # librdkafka's metadata refreshes, and one HTTP log line per Schema
    # Registry lookup.
    for noisy in ("confluent_kafka", "httpx", "httpcore", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    # librdkafka warns about conditions it then recovers from by itself - most
    # visibly, failing to acquire an idempotence PID while the transaction
    # coordinator is still loading just after the broker starts, which it
    # retries until it succeeds. Keep only genuine errors: a failure that
    # actually matters still reaches us as a delivery report or a
    # KafkaException, both of which this code handles explicitly.
    logging.getLogger(KAFKA_CLIENT_LOGGER).setLevel(logging.ERROR)
