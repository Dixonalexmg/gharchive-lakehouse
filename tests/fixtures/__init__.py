"""Synthetic GH Archive fixtures for tests (data is generated on the fly)."""

from tests.fixtures.synthetic import (
    SAMPLE_EVENT_TYPES,
    make_event,
    make_events,
    write_hour_dump,
)

__all__ = [
    "SAMPLE_EVENT_TYPES",
    "make_event",
    "make_events",
    "write_hour_dump",
]
