"""Error taxonomy that drives the retry / dead-letter decisions.

The consumer never inspects concrete exception types from third-party
libraries; everything is normalised into one of the classes below so that the
routing rule stays a single, testable statement:

    transient  -> retry with exponential backoff, then dead-letter
    permanent  -> dead-letter immediately, do not waste retries
"""

from __future__ import annotations


class ProcessingError(Exception):
    """Base class for every failure raised while handling an order."""


class TransientProcessingError(ProcessingError):
    """A failure that is expected to succeed if the same message is retried.

    Examples: a downstream HTTP 503, a database deadlock, a socket timeout.
    """


class PermanentProcessingError(ProcessingError):
    """A failure that will never succeed, no matter how often it is retried.

    Examples: a payload that violates a business rule, an unknown product.
    Retrying these only delays the message and blocks the partition, so they go
    straight to the dead letter queue.
    """


class DeserializationError(PermanentProcessingError):
    """The message bytes could not be decoded into an :class:`~order_pipeline.models.Order`.

    Corrupt bytes, a wrong magic byte, or a schema the registry cannot resolve.
    Permanent by definition: the bytes on the partition will not change.
    """


class ValidationError(PermanentProcessingError):
    """The message decoded cleanly but violates the order business rules."""


class RetryExhaustedError(ProcessingError):
    """Every retry attempt for a transient failure was used up."""

    def __init__(self, attempts: int, last_error: BaseException) -> None:
        super().__init__(f"giving up after {attempts} attempt(s): {last_error}")
        self.attempts = attempts
        self.last_error = last_error
