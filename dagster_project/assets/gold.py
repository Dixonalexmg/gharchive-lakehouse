"""Gold layer Dagster assets — orchestrates dbt models.

The whole Gold layer is materialized as a single asset (``gold_marts``)
because dbt resolves model dependencies internally; exposing per-model assets
would duplicate that DAG. Use ``dagster dev`` + ``dbt build`` (via ``make
gold``) for fine-grained debugging.
"""

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from dagster_project.assets.silver import silver_events
from src.ingestion.spark import get_spark
from src.transformation.gold import run_dbt


@asset(
    name="gold_marts",
    group_name="gold",
    deps=[silver_events],
    description=(
        "Gold dimensional model (fact_events, dim_repo, dim_actor, dim_date) "
        "built by dbt. Runs `dbt build` against the live Silver Delta."
    ),
    compute_kind="dbt",
)
def gold_marts(context: AssetExecutionContext) -> MaterializeResult:
    spark = get_spark(app_name="gharchive-gold")
    try:
        rc = run_dbt(["build"])
    finally:
        spark.stop()
    if rc != 0:
        raise RuntimeError(f"dbt build failed with exit code {rc}")
    return MaterializeResult(
        metadata={
            "exit_code": MetadataValue.int(rc),
            "framework": MetadataValue.text("dbt-spark"),
        }
    )
