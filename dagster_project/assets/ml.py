"""ML assets — repo churn classifier."""

import datetime as dt
import os

from dagster import (
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
)

from dagster_project.assets.gold import gold_marts
from src.ml.features import ChurnConfig
from src.ml.train import train_churn_model


class ChurnTrainingConfig(Config):
    """Run-time config for the churn asset.

    ``split_date`` defaults to the previous month boundary so backfills behave
    deterministically when triggered without an explicit override.
    """

    split_date: str = (dt.date.today().replace(day=1)).isoformat()
    min_events: int = 5
    experiment: str = "gharchive-churn"


@asset(
    name="churn_model",
    group_name="ml",
    deps=[gold_marts],
    description=(
        "Repo-churn binary classifier trained on Gold fact_events. Logs "
        "params/metrics/model artifact to MLflow."
    ),
    compute_kind="mlflow",
)
def churn_model(context: AssetExecutionContext, config: ChurnTrainingConfig) -> MaterializeResult:
    split_date = dt.date.fromisoformat(config.split_date)
    base = ChurnConfig.from_env(split_date)
    cfg = ChurnConfig(
        gold_path=base.gold_path,
        split_date=split_date,
        min_events_in_train=config.min_events,
    )

    experiment = os.environ.get("MLFLOW_EXPERIMENT_NAME", config.experiment)
    metrics = train_churn_model(cfg, experiment)

    return MaterializeResult(
        metadata={
            "split_date": MetadataValue.text(config.split_date),
            "experiment": MetadataValue.text(experiment),
            "accuracy": MetadataValue.float(metrics["accuracy"]),
            "roc_auc": MetadataValue.float(metrics["roc_auc"]),
            "churn_rate": MetadataValue.float(metrics["churn_rate"]),
        }
    )
