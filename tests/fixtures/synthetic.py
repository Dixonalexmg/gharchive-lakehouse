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
) -> list[dict[str, object]]:
    """Build ``count`` events incrementing by 1 minute, cycling through types."""
    return [
        make_event(
            event_id=i,
            timestamp=base_time + dt.timedelta(minutes=i),
            event_type=event_types[i % len(event_types)],
            actor_id=i + 1,
            actor_login=f"user{i}",
            repo_id=1000 + i,
            repo_name=f"user{i}/repo",
        )
        for i in range(count)
    ]


def write_hour_dump(
    dest_dir: Path,
    date: dt.date,
    hour: int,
    count: int = 10,
) -> Path:
    """Write one synthetic ``YYYY-MM-DD-H.json.gz`` file and return its path."""
    if not 0 <= hour <= 23:
        raise ValueError(f"hour must be in 0..23, got {hour}")
    dest_dir.mkdir(parents=True, exist_ok=True)
    path = dest_dir / f"{date:%Y-%m-%d}-{hour}.json.gz"
    base_time = dt.datetime(date.year, date.month, date.day, hour, 0, 0, tzinfo=dt.UTC)
    events = make_events(count, base_time)
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event) + "\n")
    return path
