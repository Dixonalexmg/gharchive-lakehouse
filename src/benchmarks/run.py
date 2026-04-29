"""Entrypoint for ``make benchmark``.

Runs each benchmark scenario in a temp directory and renders a markdown table
to stdout (and optionally appends to ``docs/performance.md``).
"""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from collections.abc import Iterable
from pathlib import Path

from src.benchmarks.scenarios import SCENARIOS, BenchmarkResult
from src.ingestion.spark import get_spark

logger = logging.getLogger(__name__)


def render_markdown(results: list[BenchmarkResult]) -> str:
    """Render results as a Github-flavored markdown table."""
    lines = [
        "| Scenario | Variant | Rows | Duration (s) | Notes |",
        "| --- | --- | ---: | ---: | --- |",
    ]
    for r in results:
        lines.append(
            f"| {r.scenario} | {r.variant} | {r.rows:,} | {r.duration_s:.3f} | {r.notes} |"
        )
    return "\n".join(lines)


def run_scenarios(
    base_path: Path, n_rows: int, names: Iterable[str] | None = None
) -> list[BenchmarkResult]:
    selected = list(names) if names else list(SCENARIOS)
    spark = get_spark(app_name="gharchive-benchmarks")
    results: list[BenchmarkResult] = []
    try:
        for name in selected:
            if name not in SCENARIOS:
                logger.warning("Unknown scenario %s — skipping", name)
                continue
            scenario_dir = base_path / name
            scenario_dir.mkdir(parents=True, exist_ok=True)
            results.extend(SCENARIOS[name](spark, scenario_dir, n_rows))
    finally:
        spark.stop()
    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run lakehouse performance benchmarks.")
    parser.add_argument("--rows", type=int, default=100_000, help="Synthetic rows per scenario.")
    parser.add_argument(
        "--scenario",
        action="append",
        choices=list(SCENARIOS),
        default=None,
        help="Restrict to specific scenarios (repeatable).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional path to append the markdown table to (e.g. docs/performance.md).",
    )
    parser.add_argument(
        "--workdir",
        type=Path,
        default=None,
        help="Persistent benchmark workspace. Defaults to a temp directory.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    if args.workdir is not None:
        args.workdir.mkdir(parents=True, exist_ok=True)
        results = run_scenarios(args.workdir, args.rows, args.scenario)
    else:
        with tempfile.TemporaryDirectory(prefix="gharchive-bench-") as tmp:
            results = run_scenarios(Path(tmp), args.rows, args.scenario)

    table = render_markdown(results)
    print(table)

    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("a", encoding="utf-8") as fh:
            fh.write("\n\n")
            fh.write(table)
            fh.write("\n")
        logger.info("Appended results to %s", args.output)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
