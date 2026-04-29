"""Generate synthetic GH Archive hourly dumps (JSONL gzipped)."""

from __future__ import annotations

import datetime as dt
import gzip
import json
from pathlib import Path

SAMPLE_EVENT_TYPES = (
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "ForkEvent",
)


def make_event(
    event_id: int,
    timestamp: dt.datetime,
    event_type: str = "PushEvent",
    actor_id: int = 1,
    actor_login: str = "alice",
    repo_id: int = 100,
    repo_name: str = "alice/repo1",
) -> dict[str, object]:
    """Build a minimal but realistic GH Archive event payload."""
    return {
        "id": str(event_id),
        "type": event_type,
        "actor": {
            "id": actor_id,
            "login": actor_login,
            "display_login": actor_login,
            "url": f"https://api.github.com/users/{actor_login}",
        },
        "repo": {
            "id": repo_id,
            "name": repo_name,
            "url": f"https://api.github.com/repos/{repo_name}",
        },
        "payload": {"ref": "refs/heads/main", "size": 1},
        "public": True,
        "created_at": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def make_events(
    count: int,
    base_time: dt.datetime,
    event_types: tuple[str, ...] = SAMPLE_EVENT_TYPES,
    id_offset: int = 0,
) -> list[dict[str, object]]:
    """Build ``count`` events incrementing by 1 minute, cycling through types.

    ``id_offset`` is added to each event id so multi-hour dumps generate
    globally unique ids — otherwise Silver's dedup-by-event_id collapses
    them and the apparent row count drops.
    """
    return [
        make_event(
            event_id=id_offset + i,
            timestamp=base_time + dt.timedelta(minutes=i),
            event_type=event_types[i % len(event_types)],
            actor_id=id_offset + i + 1,
            actor_login=f"user{id_offset + i}",
            repo_id=1000 + id_offset + i,
            repo_name=f"user{id_offset + i}/repo",
        )
        for i in range(count)
    ]


def write_hour_dump(
    dest_dir: Path,
    date: dt.date,
    hour: int,
    count: int = 10,
) -> Path:
    """Write one synthetic ``YYYY-MM-DD-H.json.gz`` file and return its path.

    Event ids are namespaced by ``(date, hour)`` so dumps for different
    hours never collide on the natural key.
    """
    if not 0 <= hour <= 23:
        raise ValueError(f"hour must be in 0..23, got {hour}")
    dest_dir.mkdir(parents=True, exist_ok=True)
    path = dest_dir / f"{date:%Y-%m-%d}-{hour}.json.gz"
    base_time = dt.datetime(date.year, date.month, date.day, hour, 0, 0, tzinfo=dt.UTC)
    # Reserve 1000 ids per hour and 24_000 per day — comfortably above any
    # reasonable test ``count`` while keeping ids small.
    day_offset = (date.toordinal() - dt.date(2024, 1, 1).toordinal()) * 24_000
    id_offset = day_offset + hour * 1000
    events = make_events(count, base_time, id_offset=id_offset)
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event) + "\n")
    return path
