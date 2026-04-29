"""End-to-end Bronze pipeline on synthetic data.

Exercises ``read_events`` + ``write_bronze`` against a real local SparkSession
and verifies that the resulting Delta table is partitioned by ``event_date``
and round-trips correctly.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from src.ingestion.gharchive import read_events, write_bronze

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.spark, pytest.mark.integration]


def test_read_events_adds_partition_and_metadata_columns(
    spark: SparkSession, synthetic_hour: Path
) -> None:
    df = read_events(spark, [synthetic_hour])
    assert df.count() == 10
    assert {"event_date", "_ingested_at", "id", "type", "created_at"} <= set(df.columns)
    rows = df.select("event_date").distinct().collect()
    assert [r["event_date"].isoformat() for r in rows] == ["2024-01-01"]


def test_read_events_rejects_empty_paths(spark: SparkSession) -> None:
    with pytest.raises(ValueError, match="at least one path"):
        read_events(spark, [])


def test_write_bronze_roundtrip_partitioned_by_event_date(
    spark: SparkSession, synthetic_day: list[Path], tmp_path: Path
) -> None:
    bronze_path = tmp_path / "bronze_gharchive_events"

    df = read_events(spark, synthetic_day)
    write_bronze(df, bronze_path)

    assert (bronze_path / "_delta_log").is_dir()
    partition_dirs = sorted(p.name for p in bronze_path.glob("event_date=*"))
    assert partition_dirs == ["event_date=2024-01-01"]

    reloaded = spark.read.format("delta").load(str(bronze_path))
    assert reloaded.count() == 15
    assert "event_date" in reloaded.columns


def test_write_bronze_rejects_invalid_mode(
    spark: SparkSession, synthetic_hour: Path, tmp_path: Path
) -> None:
    df = read_events(spark, [synthetic_hour])
    with pytest.raises(ValueError, match="mode must be"):
        write_bronze(df, tmp_path / "bronze", mode="merge")


def test_write_bronze_append_increments_rows(
    spark: SparkSession, synthetic_hour: Path, tmp_path: Path
) -> None:
    bronze_path = tmp_path / "bronze_gharchive_events"
    df = read_events(spark, [synthetic_hour])
    write_bronze(df, bronze_path)
    write_bronze(df, bronze_path, mode="append")

    reloaded = spark.read.format("delta").load(str(bronze_path))
    assert reloaded.count() == 20
