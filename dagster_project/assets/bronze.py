"""Bronze layer Dagster assets.

``bronze_events`` is a daily-partitioned asset that materializes one calendar
day of GH Archive events into the Bronze Delta table. Partition key format is
``YYYY-MM-DD``; backfills are performed via Dagster's partition UI or CLI.
"""

import datetime as dt

from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
)

from src.ingestion.config import load_ingestion_config
from src.ingestion.gharchive import ingest_day
from src.ingestion.spark import get_spark

# GH Archive started 2011-02-12. We start the partition from 2015-01-01 to keep
# backfill ranges manageable; lower this if older history is needed.
BRONZE_PARTITIONS = DailyPartitionsDefinition(start_date="2015-01-01")


@asset(
    name="bronze_events",
    group_name="bronze",
    partitions_def=BRONZE_PARTITIONS,
    description=(
        "Raw GH Archive events for one day, landed as Delta and partitioned "
        "by event_date. One materialization == 24 hourly downloads."
    ),
    compute_kind="pyspark",
)
def bronze_events(context: AssetExecutionContext) -> MaterializeResult:
    partition_date = dt.date.fromisoformat(context.partition_key)
    config = load_ingestion_config()

    context.log.info(
        "Materializing bronze_events for %s (bronze_path=%s)",
        partition_date,
        config.bronze_path,
    )

    spark = get_spark(app_name=f"gharchive-bronze-{partition_date.isoformat()}")
    try:
        rows = ingest_day(spark, partition_date, config)
    finally:
        spark.stop()

    return MaterializeResult(
        metadata={
            "partition": MetadataValue.text(partition_date.isoformat()),
            "row_count": MetadataValue.int(rows),
            "bronze_path": MetadataValue.path(str(config.bronze_path)),
            "source_url": MetadataValue.url(config.base_url),
        }
    )
