"""Quality assets — Great Expectations checkpoints per layer.

The runner is invoked once per Dagster run; it iterates over all layers and
records pass/fail metadata so failed expectations show up directly in the
asset materialization view.
"""

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from dagster_project.assets.gold import gold_marts
from src.quality.run_checkpoints import run_checkpoints


@asset(
    name="quality_checkpoints",
    group_name="quality",
    deps=[gold_marts],
    description="Run Great Expectations checkpoints for Bronze, Silver, and Gold.",
    compute_kind="great_expectations",
)
def quality_checkpoints(context: AssetExecutionContext) -> MaterializeResult:
    results = run_checkpoints()
    failed = [r for r in results if not r.success]

    metadata = {f"{r.name}_success": MetadataValue.bool(r.success) for r in results}
    metadata["layers_evaluated"] = MetadataValue.int(len(results))
    metadata["failures"] = MetadataValue.int(len(failed))

    if failed:
        names = ", ".join(r.name for r in failed)
        raise RuntimeError(f"Quality checkpoints failed for: {names}")
    return MaterializeResult(metadata=metadata)
