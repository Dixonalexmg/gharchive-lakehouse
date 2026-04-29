"""GH Archive Bronze ingestion.

Pipeline:
    1. Generate hourly URLs (`https://data.gharchive.org/{YYYY-MM-DD-H}.json.gz`)
       for the requested date range.
    2. Concurrently download with ``httpx.AsyncClient`` honoring a semaphore
       and exponential backoff. Skips files already on disk.
    3. Read the downloaded JSON.gz files with ``spark.read.json``.
    4. Append to a Bronze Delta table partitioned by ``event_date``.

Schema is intentionally inferred at Bronze: we trust gharchive's structure but
do not enforce it. Schema enforcement is the Silver layer's job.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from calendar import monthrange
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

import httpx
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812 (PySpark idiom)

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://data.gharchive.org"
DEFAULT_CONCURRENCY = 8
DEFAULT_TIMEOUT_S = 60.0
DEFAULT_RETRIES = 3
BRONZE_TABLE_DIRNAME = "bronze_gharchive_events"


@dataclass(frozen=True)
class IngestionConfig:
    """Resolved configuration for one Bronze ingestion run."""

    base_url: str = DEFAULT_BASE_URL
    raw_dir: Path = Path("./data/raw")
    bronze_path: Path = Path("./data/bronze") / BRONZE_TABLE_DIRNAME
    concurrency: int = DEFAULT_CONCURRENCY
    timeout_s: float = DEFAULT_TIMEOUT_S
    retries: int = DEFAULT_RETRIES


# ---------------------------------------------------------------------------
# URL generation
# ---------------------------------------------------------------------------


def hour_url(date: dt.date, hour: int, base_url: str = DEFAULT_BASE_URL) -> str:
    """Return the GH Archive URL for a single hour.

    GH Archive uses an unpadded hour suffix (``2024-01-01-0.json.gz``).
    """
    if not 0 <= hour <= 23:
        raise ValueError(f"hour must be in 0..23, got {hour}")
    return f"{base_url}/{date:%Y-%m-%d}-{hour}.json.gz"


def hour_urls_for_day(date: dt.date, base_url: str = DEFAULT_BASE_URL) -> list[str]:
    """Return the 24 hourly URLs for a given day."""
    return [hour_url(date, h, base_url) for h in range(24)]


def hour_urls_for_month(year: int, month: int, base_url: str = DEFAULT_BASE_URL) -> list[str]:
    """Return all hourly URLs for a calendar month."""
    _, days = monthrange(year, month)
    return [
        hour_url(dt.date(year, month, day), h, base_url)
        for day in range(1, days + 1)
        for h in range(24)
    ]


# ---------------------------------------------------------------------------
# Concurrent downloads
# ---------------------------------------------------------------------------


async def _download_one(
    client: httpx.AsyncClient,
    url: str,
    dest_dir: Path,
    semaphore: asyncio.Semaphore,
    retries: int,
) -> Path:
    filename = url.rsplit("/", 1)[-1]
    dest = dest_dir / filename
    if dest.exists() and dest.stat().st_size > 0:
        logger.debug("Skip existing %s", dest)
        return dest

    tmp = dest.with_suffix(dest.suffix + ".part")
    async with semaphore:
        for attempt in range(1, retries + 1):
            try:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    with tmp.open("wb") as fh:
                        async for chunk in response.aiter_bytes(chunk_size=65536):
                            fh.write(chunk)
                tmp.replace(dest)
                logger.info("Downloaded %s", url)
                return dest
            except httpx.HTTPError as exc:
                if attempt == retries:
                    tmp.unlink(missing_ok=True)
                    raise
                backoff = 2 ** (attempt - 1)
                logger.warning(
                    "Retry %d/%d for %s after %s (sleep %ds)",
                    attempt,
                    retries,
                    url,
                    exc,
                    backoff,
                )
                await asyncio.sleep(backoff)
    raise RuntimeError(f"unreachable: download loop exited for {url}")


async def _download_many_async(
    urls: Sequence[str],
    dest_dir: Path,
    concurrency: int,
    timeout_s: float,
    retries: int,
) -> list[Path]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    sem = asyncio.Semaphore(concurrency)
    timeout = httpx.Timeout(timeout_s)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        tasks = [_download_one(client, u, dest_dir, sem, retries) for u in urls]
        return list(await asyncio.gather(*tasks))


def download_many(
    urls: Iterable[str],
    dest_dir: Path,
    concurrency: int = DEFAULT_CONCURRENCY,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    retries: int = DEFAULT_RETRIES,
) -> list[Path]:
    """Download URLs concurrently, returning local paths.

    Files already on disk are reused. Partial downloads are written to ``*.part``
    and atomically renamed on success.
    """
    url_list = list(urls)
    if not url_list:
        return []
    return asyncio.run(_download_many_async(url_list, dest_dir, concurrency, timeout_s, retries))


# ---------------------------------------------------------------------------
# Spark read + Delta write
# ---------------------------------------------------------------------------


def read_events(spark: SparkSession, paths: Sequence[Path]) -> DataFrame:
    """Parse downloaded JSON.gz files into a Spark DataFrame.

    Adds two ingestion columns:
      * ``event_date`` — DATE derived from ``created_at`` (partition column).
      * ``_ingested_at`` — TIMESTAMP set at read time.
    """
    if not paths:
        raise ValueError("read_events requires at least one path")

    str_paths = [str(p) for p in paths]
    df = spark.read.json(str_paths)
    return df.withColumn("event_date", F.to_date(F.col("created_at"))).withColumn(
        "_ingested_at", F.current_timestamp()
    )


def write_bronze(df: DataFrame, table_path: Path, mode: str = "append") -> None:
    """Write a DataFrame to the Bronze Delta table partitioned by ``event_date``."""
    if mode not in {"append", "overwrite"}:
        raise ValueError(f"mode must be 'append' or 'overwrite', got {mode!r}")
    (
        df.write.format("delta")
        .mode(mode)
        .partitionBy("event_date")
        .option("mergeSchema", "true")
        .save(str(table_path))
    )


# ---------------------------------------------------------------------------
# High-level orchestration
# ---------------------------------------------------------------------------


def ingest_urls(
    spark: SparkSession,
    urls: Sequence[str],
    config: IngestionConfig,
    mode: str = "append",
) -> int:
    """Download the URLs and append them to Bronze. Returns rows written."""
    if not urls:
        return 0
    paths = download_many(
        urls,
        config.raw_dir,
        concurrency=config.concurrency,
        timeout_s=config.timeout_s,
        retries=config.retries,
    )
    df = read_events(spark, paths)
    row_count = df.count()
    write_bronze(df, config.bronze_path, mode=mode)
    return row_count


def ingest_day(
    spark: SparkSession, date: dt.date, config: IngestionConfig, mode: str = "append"
) -> int:
    """Download and land all 24 hours of one day into Bronze."""
    urls = hour_urls_for_day(date, config.base_url)
    return ingest_urls(spark, urls, config, mode=mode)


def ingest_month(spark: SparkSession, year: int, month: int, config: IngestionConfig) -> int:
    """Download and land all hours of a calendar month into Bronze."""
    urls = hour_urls_for_month(year, month, config.base_url)
    return ingest_urls(spark, urls, config, mode="append")
