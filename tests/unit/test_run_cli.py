"""CLI parsing for ``src.ingestion.run``."""

from __future__ import annotations

from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from src.ingestion import run as run_mod
from src.ingestion.gharchive import IngestionConfig


def test_parse_args_requires_year_and_month() -> None:
    with pytest.raises(SystemExit):
        run_mod.parse_args([])


def test_parse_args_rejects_invalid_month() -> None:
    with pytest.raises(SystemExit):
        run_mod.parse_args(["--year", "2024", "--month", "13"])


def test_parse_args_accepts_valid_inputs() -> None:
    args = run_mod.parse_args(["--year", "2024", "--month", "1", "--log-level", "DEBUG"])
    assert args.year == 2024
    assert args.month == 1
    assert args.log_level == "DEBUG"


def test_main_invokes_ingest_month_with_resolved_config(
    mocker: MockerFixture, tmp_path: Path
) -> None:
    fake_spark = mocker.MagicMock(name="SparkSession")
    mocker.patch.object(run_mod, "get_spark", return_value=fake_spark)

    fake_config = IngestionConfig(
        raw_dir=tmp_path / "raw",
        bronze_path=tmp_path / "bronze",
    )
    mocker.patch.object(run_mod, "load_ingestion_config", return_value=fake_config)
    ingest_month = mocker.patch.object(run_mod, "ingest_month", return_value=42)

    rc = run_mod.main(["--year", "2024", "--month", "3"])

    assert rc == 0
    ingest_month.assert_called_once_with(fake_spark, 2024, 3, fake_config)
    fake_spark.stop.assert_called_once()
