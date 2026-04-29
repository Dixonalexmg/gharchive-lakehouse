"""Resolve :class:`SilverConfig` from environment variables (and ``.env``)."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

from src.ingestion.gharchive import BRONZE_TABLE_DIRNAME
from src.transformation.silver import SILVER_TABLE_DIRNAME, SilverConfig


def load_silver_config() -> SilverConfig:
    """Build a :class:`SilverConfig` from env (auto-loads ``.env`` if present)."""
    load_dotenv(override=False)

    bronze_root = Path(os.environ.get("LAKE_BRONZE", "./data/bronze"))
    silver_root = Path(os.environ.get("LAKE_SILVER", "./data/silver"))

    return SilverConfig(
        bronze_path=bronze_root / BRONZE_TABLE_DIRNAME,
        silver_path=silver_root / SILVER_TABLE_DIRNAME,
    )
