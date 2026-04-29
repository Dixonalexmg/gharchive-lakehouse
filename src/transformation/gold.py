"""Run dbt build against a Delta-aware SparkSession.

dbt-spark's ``method: session`` adapter calls ``SparkSession.builder.getOrCreate()``
internally. By starting a Delta-configured session **before** invoking dbt, we
ensure the same session is reused — which keeps `make gold` runnable locally
without a separate Spark Thrift Server.

CLI:
    python -m src.transformation.gold              # dbt build (run + test)
    python -m src.transformation.gold -- run       # forward extra args to dbt
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from src.ingestion.spark import get_spark

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "dbt"


def run_dbt(argv: list[str]) -> int:
    """Invoke dbt in-process. Returns 0 on success, 1 otherwise."""
    from dbt.cli.main import dbtRunner  # noqa: PLC0415

    runner = dbtRunner()
    cli_args = [
        *argv,
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(DBT_PROJECT_DIR),
    ]
    logger.info("Invoking dbt: %s", " ".join(cli_args))
    result = runner.invoke(cli_args)
    return 0 if result.success else 1


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    raw = list(argv) if argv is not None else sys.argv[1:]
    dbt_args = raw if raw else ["build"]

    spark = get_spark(app_name="gharchive-gold")
    try:
        return run_dbt(dbt_args)
    finally:
        spark.stop()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
