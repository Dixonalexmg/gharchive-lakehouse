"""End-to-end pipeline test over a synthetic month.

Generates 30 days of synthetic GH Archive events, runs Bronze ingestion,
Silver transformation, and validates both layers against their Great
Expectations suites. Acts as a regression gate for the whole medallion
contract.

Skipped by default (``-m "not e2e"`` in pyproject). Run with::

    pytest -m e2e
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from src.ingestion.gharchive import (
    BRONZE_TABLE_DIRNAME,
    read_events,
    write_bronze,
)
from src.quality.expectations import SUITE_BUILDERS
from src.quality.run_checkpoints import validate_dataframe
from src.quality.seed_synthetic import write_hour_dump
from src.transformation.silver import (
    SILVER_TABLE_DIRNAME,
    SilverConfig,
    transform_bronze_to_silver,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.spark, pytest.mark.e2e]

DAYS = 30
HOURS_PER_DAY = 4
EVENTS_PER_HOUR = 5
EXPECTED_ROWS = DAYS * HOURS_PER_DAY * EVENTS_PER_HOUR  # 600


def _seed_month(raw_dir: Path, base_date: dt.date) -> list[Path]:
    paths: list[Path] = []
    for d in range(DAYS):
        date = base_date + dt.timedelta(days=d)
        for h in range(HOURS_PER_DAY):
            paths.append(write_hour_dump(raw_dir, date, h, EVENTS_PER_HOUR))
    return paths


def test_full_month_pipeline(spark: SparkSession, tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    bronze_path = tmp_path / "bronze" / BRONZE_TABLE_DIRNAME
    silver_path = tmp_path / "silver" / SILVER_TABLE_DIRNAME

    paths = _seed_month(raw_dir, dt.date(2024, 1, 1))
    assert len(paths) == DAYS * HOURS_PER_DAY

    df = read_events(spark, paths)
    write_bronze(df, bronze_path, mode="overwrite")

    silver_rows = transform_bronze_to_silver(
        spark, SilverConfig(bronze_path=bronze_path, silver_path=silver_path)
    )
    assert silver_rows == EXPECTED_ROWS

    silver = spark.read.format("delta").load(str(silver_path))
    assert silver.count() == EXPECTED_ROWS
    assert silver.where("event_id IS NULL").count() == 0
    assert silver.select("event_id").distinct().count() == EXPECTED_ROWS

    partitions = sorted(p.name for p in silver_path.glob("event_date=*"))
    assert len(partitions) == DAYS

    for layer, path in (("bronze", bronze_path), ("silver", silver_path)):
        layer_df = spark.read.format("delta").load(str(path))
        result = validate_dataframe(layer, layer_df, SUITE_BUILDERS[layer]())
        assert getattr(result, "success", False), f"GX checkpoint failed on {layer} layer"
