"""CLI entrypoint for ``make silver``. Runs Bronze → Silver transformation."""

from __future__ import annotations

import argparse
import logging
import sys

from src.ingestion.spark import get_spark
from src.transformation.config import load_silver_config
from src.transformation.silver import transform_bronze_to_silver

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze → Silver transformation.")
    parser.add_argument(
        "--mode",
        default="overwrite",
        choices=["overwrite", "append"],
        help="Delta write mode (default: overwrite).",
    )
    parser.add_argument(
        "--date",
        action="append",
        default=None,
        metavar="YYYY-MM-DD",
        help="Restrict to one or more event_date partitions (repeatable).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    config = load_silver_config()
    logger.info(
        "Starting Silver transform bronze=%s silver=%s mode=%s dates=%s",
        config.bronze_path,
        config.silver_path,
        args.mode,
        args.date,
    )

    spark = get_spark(app_name="gharchive-silver")
    try:
        rows = transform_bronze_to_silver(spark, config, mode=args.mode, event_dates=args.date)
    finally:
        spark.stop()

    logger.info("Silver transform complete: %d rows", rows)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
