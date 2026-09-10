"""Locating and reading the Avro schema files.

The ``.avsc`` files are the single source of truth for the wire format: they
are read at runtime rather than duplicated as Python string literals, so the
schema registered with the Schema Registry can never drift from the file that
is submitted with the assignment.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path

ORDER_SCHEMA = "order.avsc"
ORDER_AGGREGATE_SCHEMA = "order_aggregate.avsc"

# Installed wheel: order_pipeline/_schemas/. Source checkout: <repo>/schemas/.
_CANDIDATE_DIRS = (
    Path(__file__).parent / "_schemas",
    Path(__file__).parent.parent.parent / "schemas",
)


def schema_dir() -> Path:
    """Return the directory holding the ``.avsc`` files."""
    for candidate in _CANDIDATE_DIRS:
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError(
        "could not locate the Avro schema directory; looked in: "
        + ", ".join(str(path) for path in _CANDIDATE_DIRS)
    )


@cache
def load_schema(filename: str) -> str:
    """Read an Avro schema file and return it as a JSON string.

    Cached: the Avro serializer needs the same string on every message.
    """
    path = schema_dir() / filename
    if not path.is_file():
        raise FileNotFoundError(f"Avro schema not found: {path}")
    return path.read_text(encoding="utf-8")
