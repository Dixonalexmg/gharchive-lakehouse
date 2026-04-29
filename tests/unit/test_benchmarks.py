"""Unit tests for the benchmark CLI/orchestration (no Spark)."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from src.benchmarks import run as benchmarks_run
from src.benchmarks.scenarios import SCENARIOS, BenchmarkResult

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


def test_render_markdown_has_header_and_rows() -> None:
    rendered = benchmarks_run.render_markdown(
        [BenchmarkResult("partitioning", "flat", 0.512, 1234, "no part")]
    )
    assert "| Scenario | Variant" in rendered
    assert "partitioning" in rendered
    assert "1,234" in rendered
    assert "0.512" in rendered


def test_render_markdown_handles_empty() -> None:
    rendered = benchmarks_run.render_markdown([])
    # Header and separator only
    assert rendered.count("\n") == 1


def test_parse_args_defaults() -> None:
    args = benchmarks_run.parse_args([])
    assert args.rows == 100_000
    assert args.scenario is None
    assert args.output is None


def test_parse_args_scenario_repeatable() -> None:
    args = benchmarks_run.parse_args(["--scenario", "partitioning", "--scenario", "zorder"])
    assert args.scenario == ["partitioning", "zorder"]


def test_parse_args_rejects_unknown_scenario() -> None:
    with pytest.raises(SystemExit):
        benchmarks_run.parse_args(["--scenario", "unicorn"])


def test_run_scenarios_invokes_each_selected(mocker: MockerFixture, tmp_path: Path) -> None:
    spark = MagicMock()
    mocker.patch("src.benchmarks.run.get_spark", return_value=spark)
    fake = mocker.patch.dict(
        SCENARIOS,
        {"partitioning": MagicMock(return_value=[BenchmarkResult("partitioning", "flat", 0.1, 1)])},
        clear=False,
    )

    results = benchmarks_run.run_scenarios(tmp_path, 100, names=["partitioning"])

    assert len(results) == 1
    spark.stop.assert_called_once()
    del fake


def test_main_writes_output_when_given(mocker: MockerFixture, tmp_path: Path) -> None:
    out = tmp_path / "perf.md"
    mocker.patch(
        "src.benchmarks.run.run_scenarios",
        return_value=[BenchmarkResult("partitioning", "flat", 0.1, 10, "")],
    )
    rc = benchmarks_run.main(["--output", str(out), "--workdir", str(tmp_path / "wd")])
    assert rc == 0
    assert out.exists()
    text = out.read_text(encoding="utf-8")
    assert "partitioning" in text
