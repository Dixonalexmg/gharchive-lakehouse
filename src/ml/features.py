"""Feature engineering for the repo-churn model.

Reads ``fact_events`` (Gold) and produces one row per repository with:
    * Activity volume + diversity in the **train window**.
    * Composition (push / PR / issue ratios).
    * A binary ``churned`` label = 1 if the repo has zero events in the
      **holdout window**.

The split is driven by ``ChurnConfig.split_date``: events strictly before the
split form the train window; events on/after the split form the holdout
window. This makes the labeling deterministic across runs.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812

logger = logging.getLogger(__name__)

GOLD_FACT_DIRNAME = "fact_events"

FEATURE_COLUMNS: tuple[str, ...] = (
    "n_events",
    "n_actors",
    "n_event_types",
    "days_active",
    "push_ratio",
    "pr_ratio",
    "issue_ratio",
)
LABEL_COLUMN = "churned"


@dataclass(frozen=True)
class ChurnConfig:
    gold_path: Path
    split_date: dt.date
    min_events_in_train: int = 5

    @classmethod
    def from_env(cls, split_date: dt.date) -> ChurnConfig:
        gold_root = Path(os.environ.get("LAKE_GOLD", "./data/gold"))
        return cls(gold_path=gold_root / GOLD_FACT_DIRNAME, split_date=split_date)


def _train_aggregates(events: DataFrame, split_date: dt.date) -> DataFrame:
    train = events.where(F.col("event_date") < F.lit(split_date.isoformat()))
    return train.groupBy("repo_id").agg(
        F.count("*").alias("n_events"),
        F.countDistinct("actor_id").alias("n_actors"),
        F.countDistinct("event_type").alias("n_event_types"),
        F.countDistinct("event_date").alias("days_active"),
        F.sum(F.when(F.col("event_type") == "PushEvent", 1).otherwise(0)).alias("push_count"),
        F.sum(F.when(F.col("event_type") == "PullRequestEvent", 1).otherwise(0)).alias("pr_count"),
        F.sum(F.when(F.col("event_type") == "IssuesEvent", 1).otherwise(0)).alias("issue_count"),
    )


def _holdout_label(events: DataFrame, split_date: dt.date) -> DataFrame:
    holdout = events.where(F.col("event_date") >= F.lit(split_date.isoformat()))
    return (
        holdout.groupBy("repo_id")
        .agg(F.count("*").alias("holdout_events"))
        .select("repo_id", "holdout_events")
    )


def build_features(spark: SparkSession, config: ChurnConfig) -> DataFrame:
    """Return a feature + label DataFrame for the churn model.

    Repos with fewer than ``min_events_in_train`` events are dropped — they
    don't carry enough signal to learn from.
    """
    events = spark.read.format("delta").load(str(config.gold_path))

    train = _train_aggregates(events, config.split_date)
    holdout = _holdout_label(events, config.split_date)

    df = train.join(holdout, "repo_id", "left").na.fill({"holdout_events": 0})
    df = df.where(F.col("n_events") >= F.lit(config.min_events_in_train))

    df = (
        df.withColumn("push_ratio", (F.col("push_count") / F.col("n_events")).cast("double"))
        .withColumn("pr_ratio", (F.col("pr_count") / F.col("n_events")).cast("double"))
        .withColumn("issue_ratio", (F.col("issue_count") / F.col("n_events")).cast("double"))
        .withColumn(LABEL_COLUMN, (F.col("holdout_events") == 0).cast("int"))
    )

    return df.select("repo_id", *FEATURE_COLUMNS, LABEL_COLUMN)
