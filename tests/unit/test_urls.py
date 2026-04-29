"""URL generation for GH Archive hourly dumps."""

from __future__ import annotations

import datetime as dt

import pytest

from src.ingestion.gharchive import (
    DEFAULT_BASE_URL,
    hour_url,
    hour_urls_for_day,
    hour_urls_for_month,
)


def test_hour_url_uses_unpadded_hour() -> None:
    url = hour_url(dt.date(2024, 1, 1), 0)
    assert url == f"{DEFAULT_BASE_URL}/2024-01-01-0.json.gz"


def test_hour_url_pads_date_components() -> None:
    url = hour_url(dt.date(2024, 1, 5), 9)
    assert url == f"{DEFAULT_BASE_URL}/2024-01-05-9.json.gz"


def test_hour_url_accepts_custom_base() -> None:
    url = hour_url(dt.date(2024, 1, 1), 23, base_url="http://localhost:8000")
    assert url == "http://localhost:8000/2024-01-01-23.json.gz"


@pytest.mark.parametrize("invalid", [-1, 24, 100])
def test_hour_url_rejects_out_of_range_hour(invalid: int) -> None:
    with pytest.raises(ValueError, match="hour must be in 0..23"):
        hour_url(dt.date(2024, 1, 1), invalid)


def test_hour_urls_for_day_has_24_entries() -> None:
    urls = hour_urls_for_day(dt.date(2024, 1, 1))
    assert len(urls) == 24
    assert urls[0].endswith("2024-01-01-0.json.gz")
    assert urls[-1].endswith("2024-01-01-23.json.gz")


def test_hour_urls_for_month_january_has_744() -> None:
    urls = hour_urls_for_month(2024, 1)
    assert len(urls) == 31 * 24


def test_hour_urls_for_month_february_leap_year() -> None:
    urls = hour_urls_for_month(2024, 2)
    assert len(urls) == 29 * 24


def test_hour_urls_for_month_february_non_leap() -> None:
    urls = hour_urls_for_month(2023, 2)
    assert len(urls) == 28 * 24


def test_hour_urls_for_month_april_has_720() -> None:
    urls = hour_urls_for_month(2024, 4)
    assert len(urls) == 30 * 24
