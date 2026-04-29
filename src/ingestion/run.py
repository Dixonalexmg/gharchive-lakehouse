"""CLI entrypoint for ``make ingest YEAR=YYYY MONTH=MM``.

Backfills one calendar month from gharchive.org into Bronze Delta.
"""

from __future__ import annotations

import argparse
import logging
import sys

from src.ingestion.config import load_ingestion_config
from src.ingestion.gharchive import ingest_month
from src.ingestion.spark import get_spark

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GH Archive Bronze backfill (one month).")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True, choices=range(1, 13))
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

    config = load_ingestion_config()
    logger.info(
        "Starting Bronze backfill year=%d month=%02d bronze_path=%s",
        args.year,
        args.month,
        config.bronze_path,
    )

    spark = get_spark(app_name=f"gharchive-bronze-{args.year}-{args.month:02d}")
    try:
        rows = ingest_month(spark, args.year, args.month, config)
    finally:
        spark.stop()

    logger.info("Bronze backfill complete: %d rows written", rows)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
