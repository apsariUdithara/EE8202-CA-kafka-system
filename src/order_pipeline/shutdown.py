"""Cooperative shutdown on SIGINT / SIGTERM.

Both long-running apps must stop *between* messages rather than mid-flight:
the producer has to flush its delivery queue and the consumer has to commit
the offsets it has already processed. A shared flag makes that explicit.
"""

from __future__ import annotations

import logging
import signal
import threading
from types import FrameType

logger = logging.getLogger(__name__)


class ShutdownSignal:
    """A thread-safe 'please stop' flag wired to the usual termination signals."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def install(self) -> ShutdownSignal:
        """Register the signal handlers and return ``self`` for chaining."""
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._handle)
        return self

    def _handle(self, signum: int, _frame: FrameType | None) -> None:
        if self._event.is_set():
            logger.warning("second %s received - exiting now", signal.Signals(signum).name)
            raise KeyboardInterrupt
        logger.info(
            "%s received - finishing current work, then stopping", signal.Signals(signum).name
        )
        self._event.set()

    @property
    def requested(self) -> bool:
        """True once a shutdown has been asked for."""
        return self._event.is_set()

    def wait(self, timeout: float) -> bool:
        """Sleep up to ``timeout`` seconds, waking early if shutdown is requested."""
        return self._event.wait(timeout)
