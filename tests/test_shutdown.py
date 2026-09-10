"""Tests for the cooperative shutdown flag.

The handler is invoked directly rather than by raising a real signal, so the
test process's own handlers are left untouched.
"""

from __future__ import annotations

import signal
import time

import pytest

from order_pipeline.shutdown import ShutdownSignal


def test_a_fresh_signal_is_not_requested() -> None:
    assert ShutdownSignal().requested is False


def test_the_first_signal_requests_a_graceful_stop() -> None:
    shutdown = ShutdownSignal()

    shutdown._handle(signal.SIGINT, None)  # noqa: SLF001 - exercising the handler directly

    assert shutdown.requested is True


def test_a_second_signal_escalates_to_an_immediate_exit() -> None:
    """Impatient operators get what they ask for on the second Ctrl+C."""
    shutdown = ShutdownSignal()
    shutdown._handle(signal.SIGINT, None)  # noqa: SLF001

    with pytest.raises(KeyboardInterrupt):
        shutdown._handle(signal.SIGINT, None)  # noqa: SLF001


def test_wait_blocks_for_the_full_timeout_while_running() -> None:
    shutdown = ShutdownSignal()

    started = time.monotonic()
    woke_early = shutdown.wait(0.05)

    assert woke_early is False
    assert time.monotonic() - started >= 0.04


def test_wait_returns_immediately_once_shutdown_is_requested() -> None:
    """The producer's pacing sleep must not delay a shutdown by a whole tick."""
    shutdown = ShutdownSignal()
    shutdown._handle(signal.SIGTERM, None)  # noqa: SLF001

    started = time.monotonic()

    assert shutdown.wait(30.0) is True
    assert time.monotonic() - started < 1.0
