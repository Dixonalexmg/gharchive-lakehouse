"""SparkSession factory with Delta Lake configured.

Centralizes Spark configuration so ingestion, transformation and tests share
the same Delta-aware session. Honors `SPARK_MASTER` and memory env vars from
`.env` for parity with the docker-compose stack.
"""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(
    app_name: str = "gharchive-bronze",
    master: str | None = None,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Build (or reuse) a Delta-configured local SparkSession.

    Args:
        app_name: Spark application name shown in the UI.
        master: Spark master URL. Defaults to ``$SPARK_MASTER`` or ``local[*]``.
        extra_conf: Additional ``spark.*`` config to merge on top of defaults.

    Returns:
        A SparkSession with Delta SQL extensions and the Delta catalog wired in.
    """
    resolved_master = master or os.environ.get("SPARK_MASTER", "local[*]")
    driver_mem = os.environ.get("SPARK_DRIVER_MEMORY", "2g")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(resolved_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", driver_mem)
        .config("spark.driver.bindAddress", "127.0.0.1")
        # Advertise as 127.0.0.1 too, otherwise CI runners (whose hostname
        # resolves to an internal address Spark can't reach back) fail to
        # init the JavaSparkContext with "Connection refused".
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.showConsoleProgress", "false")
    )
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    return configure_spark_with_delta_pip(builder).getOrCreate()
