"""Entrypoint for ``make ml-train``. Trains a repo-churn model and logs it.

Implementation:
    * Build features from Gold (PySpark, no pandas).
    * Train a Spark ML LogisticRegression on a 70/30 split.
    * Log params, metrics, and the trained pipeline to MLflow under the
      ``MLFLOW_EXPERIMENT_NAME`` experiment.

Defaults to the ``mlflow.spark`` flavor — the model artifact is a SparkML
PipelineModel that can be reloaded by any Spark+MLflow consumer.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler

from src.ingestion.spark import get_spark
from src.ml.features import FEATURE_COLUMNS, LABEL_COLUMN, ChurnConfig, build_features

logger = logging.getLogger(__name__)


def _setup_mlflow(experiment_name: str) -> None:
    import mlflow  # noqa: PLC0415

    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)


def train_churn_model(config: ChurnConfig, experiment_name: str) -> dict[str, float]:
    """Train + log churn model. Returns metrics dict ``{accuracy, roc_auc}``."""
    import mlflow  # noqa: PLC0415
    import mlflow.spark  # noqa: PLC0415

    _setup_mlflow(experiment_name)

    spark = get_spark(app_name="gharchive-ml")
    try:
        features = build_features(spark, config).cache()
        n_total = features.count()
        if n_total == 0:
            raise RuntimeError(
                "No training rows. Run Bronze→Silver→Gold first or lower min_events_in_train."
            )
        n_churned = features.where(features[LABEL_COLUMN] == 1).count()
        churn_rate = n_churned / n_total

        train, test = features.randomSplit([0.7, 0.3], seed=42)

        assembler = VectorAssembler(inputCols=list(FEATURE_COLUMNS), outputCol="features")
        lr = LogisticRegression(
            featuresCol="features",
            labelCol=LABEL_COLUMN,
            maxIter=50,
            regParam=0.0,
        )
        pipeline = Pipeline(stages=[assembler, lr])

        with mlflow.start_run() as run:
            mlflow.log_params(
                {
                    "split_date": config.split_date.isoformat(),
                    "min_events_in_train": config.min_events_in_train,
                    "feature_columns": ",".join(FEATURE_COLUMNS),
                    "max_iter": 50,
                    "reg_param": 0.0,
                    "train_rows": train.count(),
                    "test_rows": test.count(),
                }
            )
            mlflow.log_metric("churn_rate", churn_rate)
            mlflow.log_metric("total_repos", n_total)

            model = pipeline.fit(train)
            preds = model.transform(test)

            roc = BinaryClassificationEvaluator(
                labelCol=LABEL_COLUMN, metricName="areaUnderROC"
            ).evaluate(preds)
            acc = MulticlassClassificationEvaluator(
                labelCol=LABEL_COLUMN, metricName="accuracy"
            ).evaluate(preds)

            mlflow.log_metric("roc_auc", roc)
            mlflow.log_metric("accuracy", acc)
            mlflow.spark.log_model(model, artifact_path="model")

            logger.info(
                "Run %s — accuracy=%.4f roc_auc=%.4f churn_rate=%.4f",
                run.info.run_id,
                acc,
                roc,
                churn_rate,
            )
            return {"accuracy": acc, "roc_auc": roc, "churn_rate": churn_rate}
    finally:
        spark.stop()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train repo-churn model and log to MLflow.")
    parser.add_argument(
        "--split-date",
        type=dt.date.fromisoformat,
        required=True,
        help="ISO date splitting train (before) and holdout (on/after) windows.",
    )
    parser.add_argument(
        "--min-events",
        type=int,
        default=5,
        help="Minimum train-window events for a repo to be included.",
    )
    parser.add_argument(
        "--experiment",
        default=None,
        help="MLflow experiment name. Defaults to $MLFLOW_EXPERIMENT_NAME or 'gharchive-churn'.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    load_dotenv(override=False)
    args = parse_args(argv)

    config = ChurnConfig.from_env(args.split_date)
    config = ChurnConfig(
        gold_path=config.gold_path,
        split_date=config.split_date,
        min_events_in_train=args.min_events,
    )
    experiment = args.experiment or os.environ.get("MLFLOW_EXPERIMENT_NAME", "gharchive-churn")

    train_churn_model(config, experiment)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
