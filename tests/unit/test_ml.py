"""Unit tests for ML CLI + feature config (no Spark)."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from src.ml.features import FEATURE_COLUMNS, LABEL_COLUMN, ChurnConfig
from src.ml.train import parse_args


def test_feature_columns_non_empty_and_unique() -> None:
    assert len(FEATURE_COLUMNS) > 0
    assert len(set(FEATURE_COLUMNS)) == len(FEATURE_COLUMNS)
    assert LABEL_COLUMN not in FEATURE_COLUMNS


def test_churn_config_from_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("LAKE_GOLD", str(tmp_path / "gold"))
    cfg = ChurnConfig.from_env(dt.date(2024, 2, 1))
    assert cfg.gold_path == tmp_path / "gold" / "fact_events"
    assert cfg.split_date == dt.date(2024, 2, 1)
    assert cfg.min_events_in_train == 5


def test_parse_args_requires_split_date() -> None:
    with pytest.raises(SystemExit):
        parse_args([])


def test_parse_args_parses_iso_date() -> None:
    args = parse_args(["--split-date", "2024-02-01", "--min-events", "10"])
    assert args.split_date == dt.date(2024, 2, 1)
    assert args.min_events == 10


def test_parse_args_rejects_bad_date() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--split-date", "not-a-date"])
