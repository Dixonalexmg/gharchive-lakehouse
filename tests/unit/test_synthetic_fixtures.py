"""Sanity tests for the synthetic fixture generator itself."""

from __future__ import annotations

import datetime as dt
import gzip
import json
from pathlib import Path

import pytest

from tests.fixtures import make_event, make_events, write_hour_dump


def test_make_event_has_expected_keys() -> None:
    event = make_event(1, dt.datetime(2024, 1, 1, tzinfo=dt.UTC))
    assert {"id", "type", "actor", "repo", "payload", "public", "created_at"} <= event.keys()
    assert event["id"] == "1"
    assert event["created_at"] == "2024-01-01T00:00:00Z"


def test_make_events_count_and_distinct_ids() -> None:
    events = make_events(7, dt.datetime(2024, 1, 1, tzinfo=dt.UTC))
    assert len(events) == 7
    assert len({e["id"] for e in events}) == 7


def test_write_hour_dump_creates_valid_gzip_jsonl(tmp_path: Path) -> None:
    path = write_hour_dump(tmp_path, dt.date(2024, 1, 1), hour=3, count=5)
    assert path.name == "2024-01-01-3.json.gz"
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        lines = [json.loads(line) for line in fh]
    assert len(lines) == 5
    assert all("created_at" in event for event in lines)


def test_write_hour_dump_rejects_invalid_hour(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="hour must be in 0..23"):
        write_hour_dump(tmp_path, dt.date(2024, 1, 1), hour=99)
