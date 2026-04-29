"""Dagster asset definitions for the GHArchive lakehouse."""

from dagster_project.assets.bronze import (
    BRONZE_PARTITIONS,
    bronze_events,
)
from dagster_project.assets.gold import gold_marts
from dagster_project.assets.ml import churn_model
from dagster_project.assets.quality import quality_checkpoints
from dagster_project.assets.silver import silver_events

__all__ = [
    "BRONZE_PARTITIONS",
    "bronze_events",
    "churn_model",
    "gold_marts",
    "quality_checkpoints",
    "silver_events",
]
