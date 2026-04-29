"""Bronze → Silver transformation.

Pipeline:
    1. Read Bronze Delta table (schema-on-read, raw nested structs).
    2. Project a flat, typed canonical schema (``SILVER_COLUMNS``).
    3. Drop rows missing the natural key (``event_id``) or ``created_at``.
    4. Deduplicate on ``event_id`` keeping the latest ``_ingested_at``.
    5. Write a Delta table partitioned by ``event_date`` with schema enforcement.

Silver is the contract layer. Downstream models (Gold/dbt, ML, GX) read this
table and must not have to reason about Bronze quirks (string ids, missing
fields, duplicate replays).
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

SILVER_TABLE_DIRNAME = "silver_gharchive_events"

#: Canonical Silver schema. Order matches projection in :func:`normalize`.
SILVER_SCHEMA = StructType(
    [
        StructField("event_id", LongType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("actor_id", LongType(), nullable=True),
        StructField("actor_login", StringType(), nullable=True),
        StructField("repo_id", LongType(), nullable=True),
        StructField("repo_name", StringType(), nullable=True),
        StructField("payload_json", StringType(), nullable=True),
        StructField("public", BooleanType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=False),
        StructField("event_date", DateType(), nullable=False),
        StructField("event_hour", IntegerType(), nullable=False),
        StructField("_ingested_at", TimestampType(), nullable=True),
        StructField("_silver_processed_at", TimestampType(), nullable=False),
    ]
)

SILVER_COLUMNS: tuple[str, ...] = tuple(f.name for f in SILVER_SCHEMA.fields)


@dataclass(frozen=True)
class SilverConfig:
    """Resolved configuration for a Silver run."""

    bronze_path: Path
    silver_path: Path


def read_bronze(spark: SparkSession, bronze_path: Path) -> DataFrame:
    """Load the Bronze Delta table as-is."""
    return spark.read.format("delta").load(str(bronze_path))


def normalize(bronze: DataFrame) -> DataFrame:
    """Project Bronze rows onto the canonical Silver schema.

    - Casts ``id`` to LONG (Bronze stores it as string).
    - Flattens ``actor.*`` and ``repo.*``.
    - Serializes ``payload`` to JSON (variant types vary per event type).
    - Derives ``event_date`` and ``event_hour`` from ``created_at``.
    - Drops rows missing the natural key.
    """
    created_at = F.to_timestamp(F.col("created_at"))

    projected = bronze.select(
        F.col("id").cast(LongType()).alias("event_id"),
        F.col("type").alias("event_type"),
        F.col("actor.id").cast(LongType()).alias("actor_id"),
        F.col("actor.login").alias("actor_login"),
        F.col("repo.id").cast(LongType()).alias("repo_id"),
        F.col("repo.name").alias("repo_name"),
        F.to_json(F.col("payload")).alias("payload_json"),
        F.col("public").cast(BooleanType()).alias("public"),
        created_at.alias("created_at"),
        F.to_date(created_at).alias("event_date"),
        F.hour(created_at).cast(IntegerType()).alias("event_hour"),
        F.col("_ingested_at"),
        F.current_timestamp().alias("_silver_processed_at"),
    )

    return projected.where(
        F.col("event_id").isNotNull()
        & F.col("event_type").isNotNull()
        & F.col("created_at").isNotNull()
    )


def deduplicate(events: DataFrame) -> DataFrame:
    """Keep the most recently ingested row per ``event_id``.

    GH Archive replays an hourly dump on transient failures, so a Bronze
    re-ingest can produce duplicate ``event_id`` values across runs. We keep
    the latest ``_ingested_at`` (or ``created_at`` as fallback when ingestion
    metadata is missing).
    """
    order_col = F.coalesce(F.col("_ingested_at"), F.col("created_at"))
    window = Window.partitionBy("event_id").orderBy(order_col.desc())
    return (
        events.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")
    )


def write_silver(df: DataFrame, silver_path: Path, mode: str = "overwrite") -> None:
    """Write a Silver-shaped DataFrame to Delta.

    Schema is enforced (no ``mergeSchema``), partitioned by ``event_date``,
    and column order is canonical (``SILVER_COLUMNS``). Use ``append`` for
    incremental days; ``overwrite`` is the safe default for backfills.
    """
    if mode not in {"append", "overwrite"}:
        raise ValueError(f"mode must be 'append' or 'overwrite', got {mode!r}")
    missing = set(SILVER_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing Silver columns: {sorted(missing)}")

    ordered = df.select(*SILVER_COLUMNS)
    (ordered.write.format("delta").mode(mode).partitionBy("event_date").save(str(silver_path)))


def transform_bronze_to_silver(
    spark: SparkSession,
    config: SilverConfig,
    mode: str = "overwrite",
    event_dates: Sequence[str] | None = None,
) -> int:
    """End-to-end Bronze → Silver run. Returns Silver row count.

    Args:
        spark: Active SparkSession.
        config: Bronze + Silver paths.
        mode: ``overwrite`` (default, safe for backfill) or ``append``.
        event_dates: Optional ISO date strings to filter Bronze before
            transformation (incremental runs).
    """
    bronze = read_bronze(spark, config.bronze_path)
    if event_dates:
        bronze = bronze.where(F.col("event_date").isin(list(event_dates)))

    normalized = normalize(bronze)
    deduped = deduplicate(normalized)

    write_silver(deduped, config.silver_path, mode=mode)

    rows = spark.read.format("delta").load(str(config.silver_path)).count()
    logger.info("Silver materialized: %d rows at %s", rows, config.silver_path)
    return rows
