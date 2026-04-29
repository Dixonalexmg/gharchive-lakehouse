"""Silver layer Dagster assets.

``silver_events`` is daily-partitioned (matching ``bronze_events``) so per-day
backfills run end-to-end.
"""

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from dagster_project.assets.bronze import BRONZE_PARTITIONS, bronze_events
from src.ingestion.spark import get_spark
from src.transformation.config import load_silver_config
from src.transformation.silver import transform_bronze_to_silver


@asset(
    name="silver_events",
    group_name="silver",
    partitions_def=BRONZE_PARTITIONS,
    deps=[bronze_events],
    description=(
        "Schema-enforced, deduplicated GH Archive events for one day. Reads "
        "the Bronze partition and overwrites the matching Silver partition."
    ),
    compute_kind="pyspark",
)
def silver_events(context: AssetExecutionContext) -> MaterializeResult:
    partition_date = context.partition_key
    config = load_silver_config()

    spark = get_spark(app_name=f"gharchive-silver-{partition_date}")
    try:
        rows = transform_bronze_to_silver(
            spark, config, mode="append", event_dates=[partition_date]
        )
    finally:
        spark.stop()

    return MaterializeResult(
        metadata={
            "partition": MetadataValue.text(partition_date),
            "row_count": MetadataValue.int(rows),
            "silver_path": MetadataValue.path(str(config.silver_path)),
        }
    )
