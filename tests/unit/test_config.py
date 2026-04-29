"""IngestionConfig env loader."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.ingestion.config import load_ingestion_config
from src.ingestion.gharchive import BRONZE_TABLE_DIRNAME, DEFAULT_BASE_URL


def test_defaults_when_env_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "GHARCHIVE_BASE_URL",
        "GHARCHIVE_RAW_DIR",
        "LAKE_BRONZE",
        "GHARCHIVE_CONCURRENCY",
        "GHARCHIVE_TIMEOUT_S",
        "GHARCHIVE_RETRIES",
    ):
        monkeypatch.delenv(key, raising=False)

    config = load_ingestion_config()
    assert config.base_url == DEFAULT_BASE_URL
    assert config.raw_dir == Path("./data/raw")
    assert config.bronze_path == Path("./data/bronze") / BRONZE_TABLE_DIRNAME


def test_overrides_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHARCHIVE_BASE_URL", "http://localhost:9999")
    monkeypatch.setenv("GHARCHIVE_RAW_DIR", "/tmp/raw")
    monkeypatch.setenv("LAKE_BRONZE", "/tmp/bronze")
    monkeypatch.setenv("GHARCHIVE_CONCURRENCY", "16")
    monkeypatch.setenv("GHARCHIVE_TIMEOUT_S", "120")
    monkeypatch.setenv("GHARCHIVE_RETRIES", "5")

    config = load_ingestion_config()
    assert config.base_url == "http://localhost:9999"
    assert config.raw_dir == Path("/tmp/raw")
    assert config.bronze_path == Path("/tmp/bronze") / BRONZE_TABLE_DIRNAME
    assert config.concurrency == 16
    assert config.timeout_s == 120.0
    assert config.retries == 5
