"""Unit tests for Silver helpers that don't require a SparkSession."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.transformation.config import load_silver_config
from src.transformation.run import parse_args
from src.transformation.silver import SILVER_COLUMNS, SILVER_SCHEMA


def test_silver_columns_match_schema() -> None:
    assert SILVER_COLUMNS == tuple(f.name for f in SILVER_SCHEMA.fields)


def test_silver_required_fields() -> None:
    nullability = {f.name: f.nullable for f in SILVER_SCHEMA.fields}
    assert nullability["event_id"] is False
    assert nullability["event_type"] is False
    assert nullability["created_at"] is False
    assert nullability["event_date"] is False


def test_parse_args_defaults() -> None:
    args = parse_args([])
    assert args.mode == "overwrite"
    assert args.date is None
    assert args.log_level == "INFO"


def test_parse_args_dates_repeatable() -> None:
    args = parse_args(["--date", "2024-01-01", "--date", "2024-01-02", "--mode", "append"])
    assert args.date == ["2024-01-01", "2024-01-02"]
    assert args.mode == "append"


def test_parse_args_rejects_unknown_mode() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--mode", "merge"])


def test_load_silver_config_uses_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr("src.transformation.config.load_dotenv", lambda **_: None)
    monkeypatch.setenv("LAKE_BRONZE", str(tmp_path / "bronze"))
    monkeypatch.setenv("LAKE_SILVER", str(tmp_path / "silver"))
    cfg = load_silver_config()
    assert cfg.bronze_path == tmp_path / "bronze" / "bronze_gharchive_events"
    assert cfg.silver_path == tmp_path / "silver" / "silver_gharchive_events"


def test_load_silver_config_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.transformation.config.load_dotenv", lambda **_: None)
    for var in ("LAKE_BRONZE", "LAKE_SILVER"):
        monkeypatch.delenv(var, raising=False)
    cfg = load_silver_config()
    assert cfg.bronze_path == Path("./data/bronze/bronze_gharchive_events")
    assert cfg.silver_path == Path("./data/silver/silver_gharchive_events")
    # ensure relative defaults are independent of os.getcwd quirks
    assert os.path.basename(cfg.silver_path) == "silver_gharchive_events"
