"""Benchmark scenarios.

Each scenario sets up two Delta variants of the same logical dataset and times
a representative query over both. Results are returned as
:class:`BenchmarkResult` rows so the runner can aggregate them.

Scenarios covered:
    * **Partition pruning** — filter by ``event_date`` on a partitioned vs
      flat table. Demonstrates partition elimination at scan time.
    * **Z-Order** — point-lookup by ``repo_id`` on a Z-ordered vs unoptimized
      table. Demonstrates data skipping via min/max statistics.
    * **Broadcast join** — small ``dim_repo`` join over ``fact_events`` with
      explicit broadcast hint vs default shuffle. Demonstrates join physical
      plan choice.
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BenchmarkResult:
    scenario: str
    variant: str
    duration_s: float
    rows: int
    notes: str = ""


def _time_count(label: str, df: DataFrame) -> tuple[float, int]:
    start = time.perf_counter()
    rows = df.count()
    elapsed = time.perf_counter() - start
    logger.info("[%s] count=%d elapsed=%.3fs", label, rows, elapsed)
    return elapsed, rows


def _generate_events(spark: SparkSession, n_rows: int, n_repos: int = 1000) -> DataFrame:
    """Synthetic event-grain dataset, deterministic across runs."""
    rng = random.Random(42)
    repos = [(i, f"org/repo_{i}") for i in range(n_repos)]
    rows = [
        (
            i,
            "PushEvent" if i % 2 == 0 else "PullRequestEvent",
            rng.choice(repos)[0],
            f"2024-01-{(i % 28) + 1:02d}",
            i % 24,
        )
        for i in range(n_rows)
    ]
    return spark.createDataFrame(
        rows, ["event_id", "event_type", "repo_id", "event_date", "event_hour"]
    ).withColumn("event_date", F.to_date(F.col("event_date")))


def benchmark_partitioning(
    spark: SparkSession, base_path: Path, n_rows: int = 100_000
) -> list[BenchmarkResult]:
    df = _generate_events(spark, n_rows)
    flat_path = base_path / "events_flat"
    part_path = base_path / "events_partitioned"

    df.write.format("delta").mode("overwrite").save(str(flat_path))
    df.write.format("delta").mode("overwrite").partitionBy("event_date").save(str(part_path))

    target_date = "2024-01-15"
    flat_q = (
        spark.read.format("delta")
        .load(str(flat_path))
        .where(F.col("event_date") == F.lit(target_date))
    )
    part_q = (
        spark.read.format("delta")
        .load(str(part_path))
        .where(F.col("event_date") == F.lit(target_date))
    )

    flat_t, flat_n = _time_count("partitioning/flat", flat_q)
    part_t, part_n = _time_count("partitioning/partitioned", part_q)

    return [
        BenchmarkResult("partitioning", "flat", flat_t, flat_n, "no partition column"),
        BenchmarkResult("partitioning", "partitioned", part_t, part_n, "partitionBy(event_date)"),
    ]


def benchmark_zorder(
    spark: SparkSession, base_path: Path, n_rows: int = 100_000
) -> list[BenchmarkResult]:
    df = _generate_events(spark, n_rows)
    plain_path = base_path / "events_plain"
    zorder_path = base_path / "events_zorder"

    df.write.format("delta").mode("overwrite").save(str(plain_path))
    df.write.format("delta").mode("overwrite").save(str(zorder_path))

    DeltaTable.forPath(spark, str(zorder_path)).optimize().executeZOrderBy("repo_id")

    target_repo = 42
    plain_q = (
        spark.read.format("delta")
        .load(str(plain_path))
        .where(F.col("repo_id") == F.lit(target_repo))
    )
    zorder_q = (
        spark.read.format("delta")
        .load(str(zorder_path))
        .where(F.col("repo_id") == F.lit(target_repo))
    )

    plain_t, plain_n = _time_count("zorder/plain", plain_q)
    zorder_t, zorder_n = _time_count("zorder/zordered", zorder_q)

    return [
        BenchmarkResult("zorder", "plain", plain_t, plain_n, "no optimize"),
        BenchmarkResult("zorder", "zordered", zorder_t, zorder_n, "ZORDER BY repo_id"),
    ]


def benchmark_broadcast(
    spark: SparkSession, base_path: Path, n_rows: int = 100_000
) -> list[BenchmarkResult]:
    fact = _generate_events(spark, n_rows)
    fact_path = base_path / "fact_events"
    fact.write.format("delta").mode("overwrite").save(str(fact_path))
    fact_loaded = spark.read.format("delta").load(str(fact_path))

    dim_rows = fact.select("repo_id").distinct().withColumn("repo_score", F.col("repo_id") % 100)
    dim_path = base_path / "dim_repo"
    dim_rows.write.format("delta").mode("overwrite").save(str(dim_path))
    dim = spark.read.format("delta").load(str(dim_path))

    shuffle_join = fact_loaded.join(dim, "repo_id")
    broadcast_join = fact_loaded.join(F.broadcast(dim), "repo_id")

    shuffle_t, shuffle_n = _time_count("broadcast/shuffle", shuffle_join)
    broadcast_t, broadcast_n = _time_count("broadcast/broadcast", broadcast_join)

    return [
        BenchmarkResult("broadcast", "shuffle", shuffle_t, shuffle_n, "default join strategy"),
        BenchmarkResult("broadcast", "broadcast", broadcast_t, broadcast_n, "F.broadcast(dim)"),
    ]


SCENARIOS = {
    "partitioning": benchmark_partitioning,
    "zorder": benchmark_zorder,
    "broadcast": benchmark_broadcast,
}
