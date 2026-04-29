"""Unit tests for the dbt Gold runner wrapper."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from src.transformation import gold

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


def test_run_dbt_passes_args_and_dirs(mocker: MockerFixture) -> None:
    runner_cls = mocker.patch("dbt.cli.main.dbtRunner")
    runner_instance = runner_cls.return_value
    runner_instance.invoke.return_value = MagicMock(success=True)

    rc = gold.run_dbt(["build"])

    assert rc == 0
    args = runner_instance.invoke.call_args.args[0]
    assert "build" in args
    assert "--project-dir" in args
    assert "--profiles-dir" in args


def test_run_dbt_returns_one_on_failure(mocker: MockerFixture) -> None:
    runner_cls = mocker.patch("dbt.cli.main.dbtRunner")
    runner_instance = runner_cls.return_value
    runner_instance.invoke.return_value = MagicMock(success=False)

    assert gold.run_dbt(["test"]) == 1


def test_main_defaults_to_build_and_starts_spark(mocker: MockerFixture) -> None:
    spark = MagicMock()
    mocker.patch("src.transformation.gold.get_spark", return_value=spark)
    run_dbt = mocker.patch("src.transformation.gold.run_dbt", return_value=0)

    rc = gold.main([])

    assert rc == 0
    run_dbt.assert_called_once_with(["build"])
    spark.stop.assert_called_once()


def test_main_forwards_extra_args(mocker: MockerFixture) -> None:
    spark = MagicMock()
    mocker.patch("src.transformation.gold.get_spark", return_value=spark)
    run_dbt = mocker.patch("src.transformation.gold.run_dbt", return_value=0)

    gold.main(["run", "--select", "fact_events"])

    run_dbt.assert_called_once_with(["run", "--select", "fact_events"])


def test_main_stops_spark_on_dbt_failure(mocker: MockerFixture) -> None:
    spark = MagicMock()
    mocker.patch("src.transformation.gold.get_spark", return_value=spark)
    mocker.patch("src.transformation.gold.run_dbt", side_effect=RuntimeError("boom"))

    with pytest.raises(RuntimeError, match="boom"):
        gold.main(["build"])

    spark.stop.assert_called_once()
