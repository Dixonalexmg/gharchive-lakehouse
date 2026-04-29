"""Dagster asset wiring smoke tests."""

from __future__ import annotations

from dagster_project.assets import (
    BRONZE_PARTITIONS,
    bronze_events,
    churn_model,
    gold_marts,
    quality_checkpoints,
    silver_events,
)
from dagster_project.definitions import defs


def test_bronze_events_is_partitioned_daily() -> None:
    assert bronze_events.partitions_def is BRONZE_PARTITIONS
    assert bronze_events.key.path[-1] == "bronze_events"


def test_silver_events_shares_bronze_partitions() -> None:
    assert silver_events.partitions_def is BRONZE_PARTITIONS
    assert silver_events.key.path[-1] == "silver_events"


def test_definitions_includes_all_layers() -> None:
    repo = defs.get_repository_def()
    asset_keys = {ak.path[-1] for ak in repo.assets_defs_by_key}
    expected = {
        "bronze_events",
        "silver_events",
        "gold_marts",
        "quality_checkpoints",
        "churn_model",
    }
    assert expected <= asset_keys


def test_downstream_assets_depend_on_upstream() -> None:
    def _upstream_keys(asset_def: object) -> set[str]:
        # Dagster exposes upstream deps via the spec for each asset key.
        names: set[str] = set()
        for spec in asset_def.specs:  # type: ignore[attr-defined]
            for dep in spec.deps:
                names.add(dep.asset_key.path[-1])
        return names

    assert "bronze_events" in _upstream_keys(silver_events)
    assert "silver_events" in _upstream_keys(gold_marts)
    assert "gold_marts" in _upstream_keys(quality_checkpoints)
    assert "gold_marts" in _upstream_keys(churn_model)
