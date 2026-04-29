"""Dagster Definitions entrypoint.

Loaded by `dagster dev -m dagster_project.definitions`.
"""

from __future__ import annotations

from dagster import Definitions

from dagster_project.assets import (
    bronze_events,
    churn_model,
    gold_marts,
    quality_checkpoints,
    silver_events,
)

defs = Definitions(
    assets=[
        bronze_events,
        silver_events,
        gold_marts,
        quality_checkpoints,
        churn_model,
    ],
    schedules=[],
    sensors=[],
    resources={},
)
