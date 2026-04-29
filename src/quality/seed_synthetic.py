"""Materialize tiny Bronze + Silver Delta tables for CI GX checkpoints.

CI doesn't have access to GH Archive, so we generate a small deterministic
synthetic dump, land it in Bronze (overwrite mode for idempotency) and run
the Silver transformation. The resulting Delta tables are what
:mod:`src.quality.run_checkpoints` validates in the CI ``gx`` job.

Kept under ``src.quality`` (not under ``tests/``) so it's part of the
installed package and importable from CI without adding test paths to
``sys.path``.
"""

from __future__ import annotations

import datetime as dt
import gzip
import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from src.ingestion.gharchive import (
    BRONZE_TABLE_DIRNAME,
    read_events,
    write_bronze,
)
from src.ingestion.spark import get_spark
from src.transformation.silver import (
    SILVER_TABLE_DIRNAME,
    SilverConfig,
    transform_bronze_to_silver,
)

logger = logging.getLogger(__name__)

EVENT_TYPES: tuple[str, ...] = (
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "ForkEvent",
)


def make_event(event_id: int, ts: dt.datetime) -> dict[str, object]:
    actor = f"user{event_id}"
    repo = f"{actor}/repo"
    return {
        "id": str(event_id),
        "type": EVENT_TYPES[event_id % len(EVENT_TYPES)],
        "actor": {
            "id": event_id + 1,
            "login": actor,
            "display_login": actor,
            "url": f"https://api.github.com/users/{actor}",
        },
        "repo": {
            "id": 1000 + event_id,
            "name": repo,
            "url": f"https://api.github.com/repos/{repo}",
        },
        "payload": {"ref": "refs/heads/main", "size": 1},
        "public": True,
        "created_at": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def write_hour_dump(raw_dir: Path, date: dt.date, hour: int, count: int) -> Path:
    """Write a synthetic hourly dump with globally unique ids per (date, hour)."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / f"{date:%Y-%m-%d}-{hour}.json.gz"
    base = dt.datetime(date.year, date.month, date.day, hour, tzinfo=dt.UTC)
    day_offset = (date.toordinal() - dt.date(2024, 1, 1).toordinal()) * 24_000
    id_offset = day_offset + hour * 1000
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for i in range(count):
            event = make_event(id_offset + i, base + dt.timedelta(minutes=i))
            fh.write(json.dumps(event) + "\n")
    return path


def seed(
    raw_dir: Path,
    bronze_root: Path,
    silver_root: Path,
    *,
    date: dt.date = dt.date(2024, 1, 1),
    hours: int = 2,
    events_per_hour: int = 20,
) -> int:
    """Seed Bronze + Silver Delta tables. Returns Silver row count."""
    paths = [write_hour_dump(raw_dir, date, h, events_per_hour) for h in range(hours)]
    bronze_path = bronze_root / BRONZE_TABLE_DIRNAME
    silver_path = silver_root / SILVER_TABLE_DIRNAME

    spark = get_spark(app_name="gharchive-ci-seed")
    try:
        df = read_events(spark, paths)
        write_bronze(df, bronze_path, mode="overwrite")
        rows = transform_bronze_to_silver(
            spark, SilverConfig(bronze_path=bronze_path, silver_path=silver_path)
        )
    finally:
        spark.stop()
    logger.info("Seed complete: bronze=%s silver=%s rows=%d", bronze_path, silver_path, rows)
    return rows


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")
    load_dotenv(override=False)
    raw_dir = Path(os.environ.get("GHARCHIVE_RAW_DIR", "./data/raw"))
    bronze_root = Path(os.environ.get("LAKE_BRONZE", "./data/bronze"))
    silver_root = Path(os.environ.get("LAKE_SILVER", "./data/silver"))
    seed(raw_dir, bronze_root, silver_root)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
