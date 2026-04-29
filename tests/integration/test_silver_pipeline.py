"""End-to-end Bronze → Silver pipeline on synthetic data."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from src.ingestion.gharchive import read_events, write_bronze
from src.transformation.silver import (
    SILVER_COLUMNS,
    SilverConfig,
    deduplicate,
    normalize,
    read_bronze,
    transform_bronze_to_silver,
    write_silver,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.spark, pytest.mark.integration]


def _seed_bronze(spark: SparkSession, hour_paths: list[Path], bronze_path: Path) -> None:
    df = read_events(spark, hour_paths)
    write_bronze(df, bronze_path)


def test_normalize_projects_canonical_schema(
    spark: SparkSession, synthetic_day: list[Path], tmp_path: Path
) -> None:
    bronze_path = tmp_path / "bronze"
    _seed_bronze(spark, synthetic_day, bronze_path)

    bronze = read_bronze(spark, bronze_path)
    silver = normalize(bronze)

    assert set(silver.columns) == set(SILVER_COLUMNS)
    row = silver.orderBy("event_id").first()
    assert row is not None
    assert row["event_id"] == 0
    assert row["event_type"] in {"PushEvent", "PullRequestEvent", "IssuesEvent"}
    assert row["repo_name"].endswith("/repo")
    assert row["event_date"].isoformat() == "2024-01-01"
    assert row["event_hour"] == 0
    assert row["payload_json"].startswith("{")


def test_deduplicate_keeps_latest_ingested(
    spark: SparkSession, synthetic_hour: Path, tmp_path: Path
) -> None:
    bronze_path = tmp_path / "bronze"
    df = read_events(spark, [synthetic_hour])
    write_bronze(df, bronze_path)
    write_bronze(df, bronze_path, mode="append")

    silver = deduplicate(normalize(read_bronze(spark, bronze_path)))

    assert silver.count() == 10
    assert silver.select("event_id").distinct().count() == 10


def test_write_silver_enforces_schema_and_partitions(
    spark: SparkSession, synthetic_day: list[Path], tmp_path: Path
) -> None:
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"
    _seed_bronze(spark, synthetic_day, bronze_path)

    silver = deduplicate(normalize(read_bronze(spark, bronze_path)))
    write_silver(silver, silver_path)

    assert (silver_path / "_delta_log").is_dir()
    partitions = sorted(p.name for p in silver_path.glob("event_date=*"))
    assert partitions == ["event_date=2024-01-01"]

    reloaded = spark.read.format("delta").load(str(silver_path))
    assert reloaded.count() == 15
    assert tuple(reloaded.columns) == SILVER_COLUMNS


def test_transform_end_to_end(
    spark: SparkSession, synthetic_day: list[Path], tmp_path: Path
) -> None:
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"
    _seed_bronze(spark, synthetic_day, bronze_path)

    config = SilverConfig(bronze_path=bronze_path, silver_path=silver_path)
    rows = transform_bronze_to_silver(spark, config)

    assert rows == 15
    reloaded = spark.read.format("delta").load(str(silver_path))
    assert reloaded.count() == 15
    assert reloaded.where("event_id IS NULL").count() == 0


def test_write_silver_rejects_invalid_mode(
    spark: SparkSession, synthetic_hour: Path, tmp_path: Path
) -> None:
    bronze_path = tmp_path / "bronze"
    _seed_bronze(spark, [synthetic_hour], bronze_path)
    silver = deduplicate(normalize(read_bronze(spark, bronze_path)))

    with pytest.raises(ValueError, match="mode must be"):
        write_silver(silver, tmp_path / "silver", mode="merge")


def test_write_silver_rejects_missing_columns(
    spark: SparkSession, synthetic_hour: Path, tmp_path: Path
) -> None:
    bronze_path = tmp_path / "bronze"
    _seed_bronze(spark, [synthetic_hour], bronze_path)
    silver = deduplicate(normalize(read_bronze(spark, bronze_path))).drop("payload_json")

    with pytest.raises(ValueError, match="missing Silver columns"):
        write_silver(silver, tmp_path / "silver")
