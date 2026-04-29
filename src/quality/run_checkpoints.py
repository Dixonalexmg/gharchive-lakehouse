"""Entrypoint for ``make gx``. Runs Great Expectations checkpoints per layer.

For each lakehouse layer (Bronze, Silver, Gold) the runner:
    1. Reads the Delta table.
    2. Builds an in-process GX 1.x ephemeral context, datasource and asset.
    3. Materializes the suite from :mod:`src.quality.expectations`.
    4. Runs the checkpoint and reports a structured pass/fail.

Exit code is non-zero if any layer fails its expectations, so the runner can
be wired into CI directly. Missing layer paths are skipped (with a warning),
not failed — that lets the runner be safe to invoke before all layers exist.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from src.ingestion.gharchive import BRONZE_TABLE_DIRNAME
from src.ingestion.spark import get_spark
from src.quality.expectations import SUITE_BUILDERS
from src.transformation.silver import SILVER_TABLE_DIRNAME

logger = logging.getLogger(__name__)

GOLD_FACT_TABLE = "fact_events"


@dataclass(frozen=True)
class LayerResult:
    name: str
    success: bool
    statistics: dict[str, Any]


def _layer_paths() -> dict[str, Path]:
    bronze = Path(os.environ.get("LAKE_BRONZE", "./data/bronze")) / BRONZE_TABLE_DIRNAME
    silver = Path(os.environ.get("LAKE_SILVER", "./data/silver")) / SILVER_TABLE_DIRNAME
    gold = Path(os.environ.get("LAKE_GOLD", "./data/gold")) / GOLD_FACT_TABLE
    return {"bronze": bronze, "silver": silver, "gold": gold}


def validate_dataframe(layer: str, df: Any, expectations: Sequence[Any]) -> Any:
    """Run an ad-hoc GX checkpoint on a Spark DataFrame.

    Uses an ephemeral context so the runner stays self-contained. Each call
    creates a fresh datasource + asset + suite scoped to ``layer`` to avoid
    name collisions across layers within the same process.
    """
    import great_expectations as gx  # noqa: PLC0415
    from great_expectations.checkpoint.checkpoint import Checkpoint  # noqa: PLC0415
    from great_expectations.core.expectation_suite import ExpectationSuite  # noqa: PLC0415
    from great_expectations.core.validation_definition import (  # noqa: PLC0415
        ValidationDefinition,
    )

    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_spark(name=f"{layer}_spark")
    asset = datasource.add_dataframe_asset(name=f"{layer}_asset")
    batch_def = asset.add_batch_definition_whole_dataframe(f"{layer}_whole")

    suite = ExpectationSuite(name=f"{layer}_suite")
    for exp in expectations:
        suite.add_expectation(exp)
    suite = context.suites.add(suite)

    validation_def = context.validation_definitions.add(
        ValidationDefinition(
            name=f"{layer}_validation",
            data=batch_def,
            suite=suite,
        )
    )
    checkpoint = context.checkpoints.add(
        Checkpoint(
            name=f"{layer}_checkpoint",
            validation_definitions=[validation_def],
        )
    )
    return checkpoint.run(batch_parameters={"dataframe": df})


def run_checkpoints(layers: Iterable[str] | None = None) -> list[LayerResult]:
    """Run quality checkpoints across the requested layers.

    Args:
        layers: Subset of ``{"bronze", "silver", "gold"}``. ``None`` runs all.

    Returns:
        One :class:`LayerResult` per layer that was actually validated. Missing
        Delta paths are skipped silently (logged as warnings).
    """
    load_dotenv(override=False)
    selected = list(layers) if layers else list(SUITE_BUILDERS)
    paths = _layer_paths()

    spark = get_spark(app_name="gharchive-gx")
    results: list[LayerResult] = []
    try:
        for layer in selected:
            if layer not in SUITE_BUILDERS:
                logger.warning("Unknown layer %s — skipping", layer)
                continue
            path = paths[layer]
            if not path.exists():
                logger.warning("Layer %s not found at %s — skipping", layer, path)
                continue

            df = spark.read.format("delta").load(str(path))
            expectations = SUITE_BUILDERS[layer]()
            checkpoint_result = validate_dataframe(layer, df, expectations)
            success = bool(getattr(checkpoint_result, "success", False))

            run_results = list(getattr(checkpoint_result, "run_results", {}).values())
            stats = run_results[0].statistics if run_results else {}

            logger.info("Layer %s: %s (%s)", layer, "PASS" if success else "FAIL", stats)
            results.append(LayerResult(name=layer, success=success, statistics=stats))
    finally:
        spark.stop()
    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run GX checkpoints per layer.")
    parser.add_argument(
        "--layer",
        action="append",
        choices=list(SUITE_BUILDERS),
        default=None,
        help="Restrict to one or more layers (repeatable). Defaults to all.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        default=None,
        help="If set, write a JSON report of layer results to this path (CI artifact).",
    )
    return parser.parse_args(argv)


def write_report(results: Sequence[LayerResult], path: Path) -> None:
    """Serialize layer results to a JSON file (parents created on demand)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [asdict(r) for r in results]
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    results = run_checkpoints(args.layer)
    if args.report_path is not None:
        write_report(results, args.report_path)
        logger.info("Wrote GX report to %s", args.report_path)
    if not results:
        logger.warning("No layers validated — check LAKE_* env vars and run upstream jobs first.")
        return 0
    return 0 if all(r.success for r in results) else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
