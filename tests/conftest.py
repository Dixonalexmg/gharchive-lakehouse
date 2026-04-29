"""Shared pytest fixtures.

A fixed-size local SparkSession (Delta-aware) is provided for tests marked
``spark``. Sessions are reused across the test session to avoid JVM startup
costs.
"""

from __future__ import annotations

import datetime as dt
import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tests.fixtures import write_hour_dump

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _java_available() -> bool:
    if os.environ.get("JAVA_HOME"):
        return True
    return shutil.which("java") is not None


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    if not _java_available():
        pytest.skip("Java runtime not found (set JAVA_HOME or install a JDK).")
    from src.ingestion.spark import get_spark  # noqa: PLC0415

    session = get_spark(
        app_name="gharchive-tests",
        master="local[2]",
        extra_conf={
            "spark.sql.shuffle.partitions": "4",
            "spark.ui.enabled": "false",
        },
    )
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture
def synthetic_hour(tmp_path: Path) -> Path:
    """One synthetic hourly dump (10 events, 2024-01-01 hour 0)."""
    return write_hour_dump(tmp_path, dt.date(2024, 1, 1), hour=0, count=10)


@pytest.fixture
def synthetic_day(tmp_path: Path) -> list[Path]:
    """Three synthetic hourly dumps from 2024-01-01 (hours 0, 1, 2)."""
    return [write_hour_dump(tmp_path, dt.date(2024, 1, 1), hour=h, count=5) for h in range(3)]
