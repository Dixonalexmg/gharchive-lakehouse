"""Resolve :class:`IngestionConfig` from environment variables (and ``.env``)."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

from src.ingestion.gharchive import (
    BRONZE_TABLE_DIRNAME,
    DEFAULT_BASE_URL,
    DEFAULT_CONCURRENCY,
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT_S,
    IngestionConfig,
)


def load_ingestion_config() -> IngestionConfig:
    """Build an :class:`IngestionConfig` from env (auto-loads ``.env`` if present)."""
    load_dotenv(override=False)

    raw_dir = Path(os.environ.get("GHARCHIVE_RAW_DIR", "./data/raw"))
    bronze_root = Path(os.environ.get("LAKE_BRONZE", "./data/bronze"))

    return IngestionConfig(
        base_url=os.environ.get("GHARCHIVE_BASE_URL", DEFAULT_BASE_URL),
        raw_dir=raw_dir,
        bronze_path=bronze_root / BRONZE_TABLE_DIRNAME,
        concurrency=int(os.environ.get("GHARCHIVE_CONCURRENCY", DEFAULT_CONCURRENCY)),
        timeout_s=float(os.environ.get("GHARCHIVE_TIMEOUT_S", DEFAULT_TIMEOUT_S)),
        retries=int(os.environ.get("GHARCHIVE_RETRIES", DEFAULT_RETRIES)),
    )
