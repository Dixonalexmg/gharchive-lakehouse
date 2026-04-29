"""Unit tests for the GX runner orchestration (no Spark required)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from src.quality import run_checkpoints
from src.quality.expectations import (
    KNOWN_EVENT_TYPES,
    SUITE_BUILDERS,
    bronze_expectations,
    gold_expectations,
    silver_expectations,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


def test_known_event_types_includes_core_events() -> None:
    for required in ("PushEvent", "PullRequestEvent", "IssuesEvent"):
        assert required in KNOWN_EVENT_TYPES


def test_suite_builders_cover_all_layers() -> None:
    assert set(SUITE_BUILDERS) == {"bronze", "silver", "gold"}


def test_bronze_expectations_non_empty() -> None:
    assert len(bronze_expectations()) > 0


def test_silver_expectations_include_unique_event_id() -> None:
    suite = silver_expectations()
    assert any(getattr(exp, "column", None) == "event_id" for exp in suite)


def test_gold_expectations_non_empty() -> None:
    assert len(gold_expectations()) > 0


def test_parse_args_defaults() -> None:
    args = run_checkpoints.parse_args([])
    assert args.layer is None
    assert args.log_level == "INFO"


def test_parse_args_layer_repeatable() -> None:
    args = run_checkpoints.parse_args(["--layer", "bronze", "--layer", "silver"])
    assert args.layer == ["bronze", "silver"]


def test_parse_args_rejects_unknown_layer() -> None:
    with pytest.raises(SystemExit):
        run_checkpoints.parse_args(["--layer", "platinum"])


def test_run_checkpoints_skips_missing_paths(mocker: MockerFixture, tmp_path: Path) -> None:
    mocker.patch("src.quality.run_checkpoints.load_dotenv", lambda **_: None)
    mocker.patch.dict(
        "os.environ",
        {
            "LAKE_BRONZE": str(tmp_path / "bronze"),
            "LAKE_SILVER": str(tmp_path / "silver"),
            "LAKE_GOLD": str(tmp_path / "gold"),
        },
        clear=False,
    )
    spark = MagicMock()
    mocker.patch("src.quality.run_checkpoints.get_spark", return_value=spark)

    results = run_checkpoints.run_checkpoints()

    assert results == []
    spark.stop.assert_called_once()


def test_run_checkpoints_validates_present_layer(mocker: MockerFixture, tmp_path: Path) -> None:
    mocker.patch("src.quality.run_checkpoints.load_dotenv", lambda **_: None)
    bronze_path = tmp_path / "bronze" / "bronze_gharchive_events"
    bronze_path.mkdir(parents=True)
    mocker.patch.dict(
        "os.environ",
        {"LAKE_BRONZE": str(tmp_path / "bronze")},
        clear=False,
    )

    spark = MagicMock()
    mocker.patch("src.quality.run_checkpoints.get_spark", return_value=spark)
    fake_result = MagicMock(success=True, run_results={})
    validate = mocker.patch(
        "src.quality.run_checkpoints.validate_dataframe", return_value=fake_result
    )

    results = run_checkpoints.run_checkpoints(layers=["bronze"])

    assert len(results) == 1
    assert results[0].name == "bronze"
    assert results[0].success is True
    validate.assert_called_once()


def test_main_returns_one_when_layer_fails(mocker: MockerFixture, tmp_path: Path) -> None:
    fail_result = run_checkpoints.LayerResult(
        name="silver", success=False, statistics={"unsuccessful_expectations": 1}
    )
    mocker.patch("src.quality.run_checkpoints.run_checkpoints", return_value=[fail_result])
    assert run_checkpoints.main([]) == 1


def test_main_returns_zero_when_no_layers(mocker: MockerFixture) -> None:
    mocker.patch("src.quality.run_checkpoints.run_checkpoints", return_value=[])
    assert run_checkpoints.main([]) == 0


def test_parse_args_report_path() -> None:
    args = run_checkpoints.parse_args(["--report-path", "out/gx.json"])
    assert args.report_path == Path("out/gx.json")


def test_write_report_serializes_layer_results(tmp_path: Path) -> None:
    results = [
        run_checkpoints.LayerResult(
            name="bronze",
            success=True,
            statistics={"successful_expectations": 4},
        ),
        run_checkpoints.LayerResult(
            name="silver",
            success=False,
            statistics={"unsuccessful_expectations": 1},
        ),
    ]
    target = tmp_path / "nested" / "report.json"

    run_checkpoints.write_report(results, target)

    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload == [
        {"name": "bronze", "success": True, "statistics": {"successful_expectations": 4}},
        {"name": "silver", "success": False, "statistics": {"unsuccessful_expectations": 1}},
    ]


def test_main_writes_report_when_path_given(mocker: MockerFixture, tmp_path: Path) -> None:
    fake = run_checkpoints.LayerResult(name="bronze", success=True, statistics={"x": 1})
    mocker.patch("src.quality.run_checkpoints.run_checkpoints", return_value=[fake])
    target = tmp_path / "gx-report.json"

    exit_code = run_checkpoints.main(["--report-path", str(target)])

    assert exit_code == 0
    assert target.exists()
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload[0]["name"] == "bronze"
    assert payload[0]["success"] is True
